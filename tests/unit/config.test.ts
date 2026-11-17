import { loadConfig } from "../../src/config/env.js";
import { auditKafkaTopics } from "../../src/modules/audit/audit-event.schema.js";

const baseEnvironment = {
  NODE_ENV: "test",
  REDIS_URL: "redis://localhost:6379",
  JWT_JWKS_URL: "http://localhost:3001/.well-known/jwks.json",
  JWT_ISSUER: "resolve-authentication-service",
  DATABASE_URL:
    "postgresql://audit_user:local-test-password@localhost:5433/resolve_audit",
  KAFKA_ENABLED: "false",
};

describe("environment configuration", () => {
  it("loads PostgreSQL and local Kafka-disabled defaults", () => {
    const config = loadConfig(baseEnvironment);

    expect(config.PORT).toBe(3000);
    expect(config.SERVICE_NAME).toBe("audit-service");
    expect(config.KAFKA_ENABLED).toBe(false);
    expect(config.KAFKA_FROM_BEGINNING).toBe(true);
    expect(config.KAFKA_BOOTSTRAP_SERVERS).toEqual(["localhost:9092"]);
    expect(config.KAFKA_TOPICS).toEqual(auditKafkaTopics);
  });

  it("allows the initial replay position to be disabled explicitly", () => {
    const config = loadConfig({
      ...baseEnvironment,
      KAFKA_FROM_BEGINNING: "false",
    });

    expect(config.KAFKA_FROM_BEGINNING).toBe(false);
  });

  it("rejects non-PostgreSQL database URLs", () => {
    expect(() =>
      loadConfig({
        ...baseEnvironment,
        DATABASE_URL: "mysql://localhost/audit",
      }),
    ).toThrow("Invalid environment configuration");
  });

  it("uses the documented brokers and topics when Kafka is enabled without overrides", () => {
    const config = loadConfig({ ...baseEnvironment, KAFKA_ENABLED: "true" });
    expect(config.KAFKA_BOOTSTRAP_SERVERS).toEqual(["localhost:9092"]);
    expect(config.KAFKA_TOPICS).toEqual(auditKafkaTopics);
  });

  it("requires Kafka brokers and topics when explicitly emptied", () => {
    expect(() =>
      loadConfig({
        ...baseEnvironment,
        KAFKA_ENABLED: "true",
        KAFKA_BOOTSTRAP_SERVERS: "",
        KAFKA_TOPICS: "",
      }),
    ).toThrow("Invalid environment configuration");
  });

  it("rejects consumer group or topic overrides that diverge from the canonical contract", () => {
    expect(() =>
      loadConfig({
        ...baseEnvironment,
        KAFKA_CONSUMER_GROUP_ID: "other-group",
      }),
    ).toThrow("Invalid environment configuration");

    expect(() =>
      loadConfig({
        ...baseEnvironment,
        KAFKA_ENABLED: "true",
        KAFKA_TOPICS: "resolve.case.created",
      }),
    ).toThrow("Invalid environment configuration");
  });

  it("requires Kafka in production", () => {
    expect(() =>
      loadConfig({
        NODE_ENV: "production",
        DATABASE_URL: baseEnvironment.DATABASE_URL,
        KAFKA_ENABLED: "false",
      }),
    ).toThrow("Invalid environment configuration");
  });
});
