import pino from "pino";
import type { EachMessagePayload } from "kafkajs";
import { loadConfig } from "../../src/config/env.js";
import { auditKafkaTopics } from "../../src/modules/audit/audit-event.schema.js";
import type { AuditIngestionService } from "../../src/modules/audit/audit-ingestion.service.js";
import {
  createKafkaConnection,
  type KafkaConsumerClient,
} from "../../src/infrastructure/kafka/client.js";

function config() {
  return loadConfig({
    NODE_ENV: "test",
    REDIS_URL: "redis://localhost:6379",
    JWT_JWKS_URL: "http://localhost:3001/.well-known/jwks.json",
    DATABASE_URL:
      "postgresql://audit_user:local-test-password@localhost:5433/resolve_audit",
    KAFKA_ENABLED: "true",
    KAFKA_BOOTSTRAP_SERVERS: "broker:9092",
    LOG_LEVEL: "silent",
  });
}

function consumerMock() {
  let crashListener:
    ((event: { error: Error; restart: boolean }) => void) | undefined;
  let readyListener: (() => void) | undefined;
  const consumer: KafkaConsumerClient = {
    connect: jest.fn().mockResolvedValue(undefined),
    subscribe: jest.fn().mockResolvedValue(undefined),
    run: jest.fn().mockResolvedValue(undefined),
    commitOffsets: jest.fn().mockResolvedValue(undefined),
    stop: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    onCrash: jest.fn((listener) => {
      crashListener = listener;
    }),
    onReady: jest.fn((listener) => {
      readyListener = listener;
    }),
  };
  return {
    consumer,
    emitCrash(event: { error: Error; restart: boolean }) {
      crashListener?.(event);
    },
    emitReady() {
      readyListener?.();
    },
  };
}

function kafkaMessage(): EachMessagePayload {
  return {
    topic: auditKafkaTopics[0]!,
    partition: 3,
    message: {
      offset: "99",
      timestamp: "2026-09-08T10:00:00.000Z",
      value: Buffer.from("{}"),
      key: null,
      attributes: 0,
      headers: {},
    },
    heartbeat: jest.fn().mockResolvedValue(undefined),
    pause: jest.fn(() => jest.fn()),
  };
}

describe("Audit Kafka consumer", () => {
  it("subscribes from the configured replay position and commits after durable persistence", async () => {
    const { consumer } = consumerMock();
    const order: string[] = [];
    const ingestion: AuditIngestionService = {
      processMessage: jest.fn(async () => {
        order.push("persist");
        return "PROCESSED";
      }),
    };
    consumer.run = jest.fn(async ({ eachMessage }) => {
      await eachMessage(kafkaMessage());
    });
    consumer.commitOffsets = jest.fn(async () => {
      order.push("commit");
    });
    const connection = createKafkaConnection(
      config(),
      ingestion,
      pino({ level: "silent" }),
      () => consumer,
    );

    await connection.start();

    expect(consumer.subscribe).toHaveBeenCalledWith({
      topics: auditKafkaTopics,
      fromBeginning: true,
    });
    expect(consumer.run).toHaveBeenCalledWith(
      expect.objectContaining({
        autoCommit: false,
        partitionsConsumedConcurrently: 3,
      }),
    );
    expect(order).toEqual(["persist", "commit"]);
    expect(consumer.commitOffsets).toHaveBeenCalledWith([
      { topic: auditKafkaTopics[0], partition: 3, offset: "100" },
    ]);
  });

  it("allows first-start replay to be disabled explicitly", async () => {
    const { consumer } = consumerMock();
    const configured = loadConfig({
      NODE_ENV: "test",
      REDIS_URL: "redis://localhost:6379",
      JWT_JWKS_URL: "http://localhost:3001/.well-known/jwks.json",
      DATABASE_URL: "postgresql://localhost:5433/resolve_audit",
      KAFKA_ENABLED: "true",
      KAFKA_FROM_BEGINNING: "false",
      KAFKA_BOOTSTRAP_SERVERS: "broker:9092",
      LOG_LEVEL: "silent",
    });
    const connection = createKafkaConnection(
      configured,
      { processMessage: jest.fn().mockResolvedValue("PROCESSED") },
      pino({ level: "silent" }),
      () => consumer,
    );

    await connection.start();

    expect(consumer.subscribe).toHaveBeenCalledWith({
      topics: auditKafkaTopics,
      fromBeginning: false,
    });
  });

  it("does not commit an offset when PostgreSQL persistence fails", async () => {
    const { consumer } = consumerMock();
    const ingestion: AuditIngestionService = {
      processMessage: jest
        .fn()
        .mockRejectedValue(new Error("database unavailable")),
    };
    consumer.run = jest.fn(async ({ eachMessage }) =>
      eachMessage(kafkaMessage()),
    );
    const connection = createKafkaConnection(
      config(),
      ingestion,
      pino({ level: "silent" }),
      () => consumer,
    );

    await expect(connection.start()).rejects.toMatchObject({
      message: "database unavailable",
      retriable: true,
    });
    expect(consumer.commitOffsets).not.toHaveBeenCalled();
    expect(consumer.disconnect).toHaveBeenCalled();
  });

  it("retries a failed database message after recovery without acknowledging it early", async () => {
    const { consumer } = consumerMock();
    const processMessage = jest
      .fn()
      .mockRejectedValueOnce(new Error("database unavailable"))
      .mockResolvedValueOnce("PROCESSED");
    const ingestion: AuditIngestionService = { processMessage };
    const connection = createKafkaConnection(
      config(),
      ingestion,
      pino({ level: "silent" }),
      () => consumer,
    );
    consumer.run = jest.fn(async ({ eachMessage }) =>
      eachMessage(kafkaMessage()),
    );

    await expect(connection.start()).rejects.toMatchObject({ retriable: true });
    expect(consumer.commitOffsets).not.toHaveBeenCalled();

    await connection.start();

    expect(processMessage).toHaveBeenCalledTimes(2);
    expect(consumer.commitOffsets).toHaveBeenCalledWith([
      { topic: auditKafkaTopics[0], partition: 3, offset: "100" },
    ]);
  });

  it("commits durable invalid-message quarantine outcomes", async () => {
    const { consumer } = consumerMock();
    const order: string[] = [];
    consumer.run = jest.fn(async ({ eachMessage }) =>
      eachMessage(kafkaMessage()),
    );
    const connection = createKafkaConnection(
      config(),
      {
        processMessage: jest.fn(async () => {
          order.push("quarantine");
          return "INVALID";
        }),
      },
      pino({ level: "silent" }),
      () => consumer,
    );
    consumer.commitOffsets = jest.fn(async () => {
      order.push("commit");
    });

    await connection.start();

    expect(order).toEqual(["quarantine", "commit"]);
  });

  it("commits redelivered duplicate events only after ingestion handles them idempotently", async () => {
    const { consumer } = consumerMock();
    const processMessage = jest
      .fn()
      .mockResolvedValueOnce("PROCESSED")
      .mockResolvedValueOnce("DUPLICATE");
    const ingestion: AuditIngestionService = { processMessage };
    consumer.run = jest.fn(async ({ eachMessage }) => {
      await eachMessage(kafkaMessage());
      await eachMessage(kafkaMessage());
    });
    const connection = createKafkaConnection(
      config(),
      ingestion,
      pino({ level: "silent" }),
      () => consumer,
    );

    await connection.start();

    expect(processMessage).toHaveBeenCalledTimes(2);
    expect(consumer.commitOffsets).toHaveBeenCalledTimes(2);
    expect(consumer.commitOffsets).toHaveBeenLastCalledWith([
      { topic: auditKafkaTopics[0], partition: 3, offset: "100" },
    ]);
  });

  it("serializes each partition and never commits a later offset before an earlier one", async () => {
    const { consumer } = consumerMock();
    let releaseFirst!: () => void;
    const firstPersistence = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const order: string[] = [];
    const ingestion: AuditIngestionService = {
      processMessage: jest.fn(async ({ offset }) => {
        order.push(`persist-${offset}`);
        if (offset === "99") await firstPersistence;
        return "PROCESSED";
      }),
    };
    consumer.run = jest.fn(async ({ eachMessage }) => {
      const first = kafkaMessage();
      const second = kafkaMessage();
      second.message.offset = "100";
      const firstHandled = eachMessage(first);
      const secondHandled = eachMessage(second);
      await Promise.resolve();
      expect(order).toEqual(["persist-99"]);
      releaseFirst();
      await Promise.all([firstHandled, secondHandled]);
    });
    consumer.commitOffsets = jest.fn(async (offsets) => {
      order.push(`commit-${offsets[0]?.offset}`);
    });
    const connection = createKafkaConnection(
      config(),
      ingestion,
      pino({ level: "silent" }),
      () => consumer,
    );

    await connection.start();

    expect(order).toEqual([
      "persist-99",
      "commit-100",
      "persist-100",
      "commit-101",
    ]);
  });

  it("does not process or commit later same-partition messages after an earlier failure", async () => {
    const { consumer } = consumerMock();
    const processMessage = jest
      .fn()
      .mockRejectedValue(new Error("transaction rolled back"));
    const ingestion: AuditIngestionService = { processMessage };
    consumer.run = jest.fn(async ({ eachMessage }) => {
      const first = kafkaMessage();
      const second = kafkaMessage();
      second.message.offset = "100";
      await Promise.all([eachMessage(first), eachMessage(second)]);
    });
    const connection = createKafkaConnection(
      config(),
      ingestion,
      pino({ level: "silent" }),
      () => consumer,
    );

    await expect(connection.start()).rejects.toMatchObject({ retriable: true });

    expect(processMessage).toHaveBeenCalledTimes(1);
    expect(consumer.commitOffsets).not.toHaveBeenCalled();
  });

  it("replays a durably processed message when its Kafka offset commit fails", async () => {
    const { consumer } = consumerMock();
    const processMessage = jest
      .fn()
      .mockResolvedValueOnce("PROCESSED")
      .mockResolvedValueOnce("DUPLICATE");
    const ingestion: AuditIngestionService = { processMessage };
    consumer.run = jest.fn(async ({ eachMessage }) =>
      eachMessage(kafkaMessage()),
    );
    consumer.commitOffsets = jest
      .fn()
      .mockRejectedValueOnce(new Error("broker unavailable"))
      .mockResolvedValueOnce(undefined);
    const connection = createKafkaConnection(
      config(),
      ingestion,
      pino({ level: "silent" }),
      () => consumer,
    );

    await expect(connection.start()).rejects.toMatchObject({ retriable: true });
    await connection.start();

    expect(processMessage).toHaveBeenCalledTimes(2);
    expect(consumer.commitOffsets).toHaveBeenCalledTimes(2);
    expect(consumer.commitOffsets).toHaveBeenLastCalledWith([
      { topic: auditKafkaTopics[0], partition: 3, offset: "100" },
    ]);
  });

  it("processes separate partitions concurrently", async () => {
    const { consumer } = consumerMock();
    const started: number[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ingestion: AuditIngestionService = {
      processMessage: jest.fn(async ({ partition }) => {
        started.push(partition);
        await gate;
        return "PROCESSED";
      }),
    };
    consumer.run = jest.fn(async ({ eachMessage }) => {
      const first = kafkaMessage();
      const second = kafkaMessage();
      second.partition = 4;
      const firstHandled = eachMessage(first);
      const secondHandled = eachMessage(second);
      await Promise.resolve();
      expect(started).toEqual([3, 4]);
      release();
      await Promise.all([firstHandled, secondHandled]);
    });
    const connection = createKafkaConnection(
      config(),
      ingestion,
      pino({ level: "silent" }),
      () => consumer,
    );

    await connection.start();

    expect(consumer.commitOffsets).toHaveBeenCalledTimes(2);
  });

  it("reports a restarting consumer as unhealthy until it rejoins its group", async () => {
    const { consumer, emitCrash, emitReady } = consumerMock();
    const connection = createKafkaConnection(
      config(),
      { processMessage: jest.fn().mockResolvedValue("PROCESSED") },
      pino({ level: "silent" }),
      () => consumer,
    );

    await connection.start();
    emitCrash({ error: new Error("broker disconnected"), restart: true });
    await expect(connection.checkHealth()).rejects.toThrow(
      "consumer is restarting",
    );

    emitReady();
    await expect(connection.checkHealth()).resolves.toBeUndefined();

    emitCrash({ error: new Error("consumer stopped"), restart: false });
    await connection.checkHealth();

    expect(consumer.connect).toHaveBeenCalledTimes(2);
    expect(consumer.subscribe).toHaveBeenCalledTimes(2);
    expect(consumer.run).toHaveBeenCalledTimes(2);
  });

  it("stops in-flight consumption before disconnecting gracefully", async () => {
    const { consumer } = consumerMock();
    const order: string[] = [];
    consumer.stop = jest.fn(async () => {
      order.push("stop");
    });
    consumer.disconnect = jest.fn(async () => {
      order.push("disconnect");
    });
    const connection = createKafkaConnection(
      config(),
      { processMessage: jest.fn().mockResolvedValue("PROCESSED") },
      pino({ level: "silent" }),
      () => consumer,
    );

    await connection.start();
    await connection.disconnect();

    expect(order).toEqual(["stop", "disconnect"]);
  });
});
