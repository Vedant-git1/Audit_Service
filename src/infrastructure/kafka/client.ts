import {
  Kafka,
  KafkaJSError,
  logLevel,
  type Consumer,
  type ConsumerConfig,
  type EachMessagePayload,
} from "kafkajs";
import type { AppConfig } from "../../config/env.js";
import type { Logger } from "pino";
import type { AuditIngestionService } from "../../modules/audit/audit-ingestion.service.js";
import { auditKafkaTopics } from "../../modules/audit/audit-event.schema.js";

export interface KafkaConnection {
  start(): Promise<void>;
  connect(): Promise<void>;
  checkHealth(): Promise<void>;
  disconnect(): Promise<void>;
}

type AuditMessageHandler = (message: EachMessagePayload) => Promise<void>;

export interface KafkaConsumerClient {
  connect(): Promise<void>;
  subscribe(subscription: {
    topics: string[];
    fromBeginning: boolean;
  }): Promise<void>;
  run(config: {
    autoCommit: boolean;
    partitionsConsumedConcurrently: number;
    eachMessage: AuditMessageHandler;
  }): Promise<void>;
  commitOffsets(
    offsets: Array<{ topic: string; partition: number; offset: string }>,
  ): Promise<void>;
  stop(): Promise<void>;
  disconnect(): Promise<void>;
  onCrash(listener: (event: { error: Error; restart: boolean }) => void): void;
  onReady(listener: () => void): void;
}

export type KafkaConsumerFactory = (
  config: ConsumerConfig,
) => KafkaConsumerClient;

export function createKafkaConnection(
  config: AppConfig,
  ingestion: AuditIngestionService,
  logger: Logger,
  consumerFactory?: KafkaConsumerFactory,
): KafkaConnection {
  if (!config.KAFKA_ENABLED) {
    return {
      async start() {},
      async connect() {},
      async checkHealth() {},
      async disconnect() {},
    };
  }

  const consumerConfig: ConsumerConfig = {
    groupId: config.KAFKA_CONSUMER_GROUP_ID,
    allowAutoTopicCreation: false,
    retry: {
      restartOnFailure: async () => true,
    },
  };
  let consumer: KafkaConsumerClient;
  if (consumerFactory) {
    consumer = consumerFactory(consumerConfig);
  } else {
    const kafkaConsumer: Consumer = new Kafka({
      clientId: config.KAFKA_CLIENT_ID,
      brokers: config.KAFKA_BOOTSTRAP_SERVERS,
      logLevel: logLevel.WARN,
    }).consumer(consumerConfig);
    consumer = {
      connect: () => kafkaConsumer.connect(),
      subscribe: (subscription) => kafkaConsumer.subscribe(subscription),
      run: (runConfig) => kafkaConsumer.run(runConfig),
      commitOffsets: (offsets) => kafkaConsumer.commitOffsets(offsets),
      stop: () => kafkaConsumer.stop(),
      disconnect: () => kafkaConsumer.disconnect(),
      onCrash: (listener) => {
        kafkaConsumer.on(kafkaConsumer.events.CRASH, (event) => {
          listener({
            error: event.payload.error,
            restart: event.payload.restart,
          });
        });
      },
      onReady: (listener) => {
        kafkaConsumer.on(kafkaConsumer.events.GROUP_JOIN, listener);
      },
    };
  }
  let connected = false;
  let subscribed = false;
  let runStarted = false;
  let healthy = false;
  let resetAfterCrash = false;
  let connectionAttempt: Promise<void> | undefined;
  const partitionQueues = new Map<string, Promise<void>>();
  consumer.onCrash(({ error, restart }) => {
    logger.error({ err: error, restart }, "Audit Kafka consumer crashed");
    healthy = false;
    if (!restart) {
      connected = false;
      subscribed = false;
      runStarted = false;
      resetAfterCrash = true;
    }
  });
  consumer.onReady(() => {
    healthy = true;
    logger.info("Audit Kafka consumer joined its group");
  });

  async function connect(): Promise<void> {
    if (healthy || runStarted) return;
    if (connectionAttempt) return connectionAttempt;

    connectionAttempt = startConsumer();
    try {
      await connectionAttempt;
    } finally {
      connectionAttempt = undefined;
    }
  }

  async function startConsumer(): Promise<void> {
    try {
      if (resetAfterCrash) {
        await consumer.disconnect();
        connected = false;
        subscribed = false;
        resetAfterCrash = false;
      }

      if (!connected) {
        await consumer.connect();
        connected = true;
      }

      if (!subscribed) {
        await consumer.subscribe({
          topics: auditKafkaTopics,
          fromBeginning: config.KAFKA_FROM_BEGINNING,
        });
        subscribed = true;
      }

      await consumer.run({
        autoCommit: false,
        partitionsConsumedConcurrently: 3,
        eachMessage: async ({ topic, partition, message, heartbeat }) => {
          const partitionKey = `${topic}:${partition}`;
          const previous =
            partitionQueues.get(partitionKey) ?? Promise.resolve();
          const current = previous.then(async () => {
            await ingestion.processMessage({
              topic,
              partition,
              offset: message.offset,
              value: message.value,
              headers: message.headers,
            });
            await heartbeat();
            const nextOffset = (BigInt(message.offset) + 1n).toString();
            await consumer.commitOffsets([
              { topic, partition, offset: nextOffset },
            ]);
          });
          partitionQueues.set(partitionKey, current);
          try {
            await current;
          } catch (error) {
            throw new KafkaJSError(
              error instanceof Error ? error : String(error),
              { retriable: true, topic, partitionId: partition },
            );
          } finally {
            if (partitionQueues.get(partitionKey) === current) {
              partitionQueues.delete(partitionKey);
            }
          }
        },
      });
      runStarted = true;
      logger.info(
        {
          topics: auditKafkaTopics,
          consumerGroup: config.KAFKA_CONSUMER_GROUP_ID,
          fromBeginning: config.KAFKA_FROM_BEGINNING,
        },
        "Audit Kafka consumer started with manual offset commits",
      );
    } catch (error) {
      logger.error({ err: error }, "Failed to start Audit Kafka consumer");
      let disconnectError: unknown;
      if (connected) {
        try {
          await consumer.disconnect();
        } catch (cleanupError) {
          disconnectError = cleanupError;
          logger.error(
            { err: cleanupError },
            "Failed to disconnect Kafka after startup failure",
          );
        }
      }
      connected = false;
      subscribed = false;
      runStarted = false;
      healthy = false;
      partitionQueues.clear();
      if (disconnectError) {
        throw new AggregateError(
          [error, disconnectError],
          "Kafka startup and cleanup both failed",
        );
      }
      throw error;
    }
  }

  return {
    start: connect,
    connect,
    async checkHealth() {
      if (runStarted && !healthy) {
        throw new Error("Audit Kafka consumer is restarting");
      }
      await connect();
    },
    async disconnect() {
      if (connected || runStarted || resetAfterCrash) {
        let stopError: unknown;
        if (runStarted) {
          try {
            await consumer.stop();
          } catch (error) {
            stopError = error;
          }
        }
        await Promise.allSettled([...partitionQueues.values()]);
        try {
          await consumer.disconnect();
        } finally {
          connected = false;
          subscribed = false;
          runStarted = false;
          healthy = false;
          resetAfterCrash = false;
          partitionQueues.clear();
        }
        if (stopError) {
          throw stopError;
        }
        logger.info("Audit Kafka consumer disconnected");
      }
    },
  };
}
