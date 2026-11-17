import "dotenv/config";
import { z } from "zod";
import { auditKafkaTopics } from "../modules/audit/audit-event.schema.js";

const booleanFromEnvironment = (defaultValue: "true" | "false") =>
  z
    .enum(["true", "false"])
    .default(defaultValue)
    .transform((value) => value === "true");

const listFromEnvironment = (defaultValue = "") =>
  z
    .string()
    .default(defaultValue)
    .transform((value) =>
      value
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean),
    );

const environmentSchema = z
  .object({
    NODE_ENV: z
      .enum(["development", "test", "production"])
      .default("development"),
    PORT: z.coerce.number().int().min(1).max(65535).default(3000),
    SERVICE_NAME: z.string().min(1).default("audit-service"),
    LOG_LEVEL: z
      .enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"])
      .default("info"),
    DATABASE_URL: z
      .string({ required_error: "DATABASE_URL is required" })
      .url("DATABASE_URL must be a valid PostgreSQL URL")
      .refine(
        (value) =>
          value.startsWith("postgresql://") || value.startsWith("postgres://"),
        "DATABASE_URL must use the PostgreSQL protocol",
      ),
    REDIS_URL: z.string().url("REDIS_URL must be a valid URL"),
    JWT_ISSUER: z.string().min(1).default("resolve-authentication-service"),
    JWT_JWKS_URL: z.string().url("JWT_JWKS_URL must be a valid URL"),
    JWT_AUDIENCE: z.string().min(1).optional(),
    KAFKA_ENABLED: booleanFromEnvironment("false"),
    KAFKA_FROM_BEGINNING: booleanFromEnvironment("true"),
    KAFKA_BOOTSTRAP_SERVERS: listFromEnvironment("localhost:9092"),
    KAFKA_CLIENT_ID: z.string().min(1).default("audit-service"),
    KAFKA_CONSUMER_GROUP_ID: z
      .string()
      .refine(
        (value) => value === "resolve-audit-service",
        "Consumer group must be resolve-audit-service",
      )
      .default("resolve-audit-service"),
    KAFKA_TOPICS: listFromEnvironment(auditKafkaTopics.join(",")),
  })
  .superRefine((config, context) => {
    if (config.KAFKA_ENABLED && config.KAFKA_BOOTSTRAP_SERVERS.length === 0) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["KAFKA_BOOTSTRAP_SERVERS"],
        message:
          "At least one Kafka bootstrap server is required when Kafka is enabled",
      });
    }

    if (config.KAFKA_ENABLED && config.KAFKA_TOPICS.length === 0) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["KAFKA_TOPICS"],
        message:
          "At least one documented Kafka topic is required when Kafka is enabled",
      });
    }

    if (
      config.KAFKA_ENABLED &&
      (config.KAFKA_TOPICS.length !== auditKafkaTopics.length ||
        auditKafkaTopics.some((topic) => !config.KAFKA_TOPICS.includes(topic)))
    ) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["KAFKA_TOPICS"],
        message:
          "KAFKA_TOPICS must contain exactly the ten documented Audit topics",
      });
    }

    if (config.NODE_ENV === "production" && !config.KAFKA_ENABLED) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["KAFKA_ENABLED"],
        message: "Kafka must be enabled in production",
      });
    }
  });

export type AppConfig = z.infer<typeof environmentSchema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const result = environmentSchema.safeParse(env);
  if (!result.success) {
    throw new Error(
      `Invalid environment configuration: ${result.error.message}`,
    );
  }
  return result.data;
}
