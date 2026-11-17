import {
  extractEventId,
  InvalidAuditEventError,
  parseJsonEvent,
  validateAuditEvent,
} from "./audit-event.schema.js";
import type {
  AuditProcessingOutcome,
  AuditRepository,
  KafkaHeaders,
  KafkaCoordinates,
} from "./audit.repository.js";

export interface KafkaAuditMessage extends KafkaCoordinates {
  value: Buffer | null;
  headers?: KafkaHeaders;
}

export interface AuditIngestionService {
  processMessage(message: KafkaAuditMessage): Promise<AuditProcessingOutcome>;
}

function validateCoordinates(coordinates: KafkaCoordinates): void {
  if (
    !coordinates.topic ||
    !Number.isInteger(coordinates.partition) ||
    coordinates.partition < 0
  ) {
    throw new Error("Kafka message has invalid topic or partition coordinates");
  }
  if (!/^(0|[1-9]\d*)$/.test(coordinates.offset)) {
    throw new Error("Kafka message has an invalid offset");
  }
}

export function createAuditIngestionService(
  repository: AuditRepository,
): AuditIngestionService {
  return {
    async processMessage(
      message: KafkaAuditMessage,
    ): Promise<AuditProcessingOutcome> {
      validateCoordinates(message);

      let rawEvent: unknown;
      try {
        rawEvent = parseJsonEvent(message.value);
        const event = validateAuditEvent(message.topic, rawEvent);
        return await repository.persistEvent(message, event);
      } catch (error) {
        if (!(error instanceof InvalidAuditEventError)) throw error;
        await repository.recordInvalid(
          message,
          extractEventId(rawEvent),
          error.message,
          message.value,
        );
        return "INVALID";
      }
    },
  };
}
