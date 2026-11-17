import { Prisma, type PrismaClient } from "@prisma/client";
import type { ValidatedAuditEvent } from "./audit-event.schema.js";

export type KafkaHeaderValue =
  Buffer | string | Array<Buffer | string> | undefined;

export interface KafkaHeaders {
  [key: string]: KafkaHeaderValue;
}

export interface KafkaCoordinates {
  topic: string;
  partition: number;
  offset: string;
  headers?: KafkaHeaders;
}

export type AuditProcessingOutcome = "PROCESSED" | "DUPLICATE" | "INVALID";

export interface AuditRepository {
  persistEvent(
    coordinates: KafkaCoordinates,
    event: ValidatedAuditEvent,
  ): Promise<AuditProcessingOutcome>;
  recordInvalid(
    coordinates: KafkaCoordinates,
    eventId: string | null,
    errorMessage: string,
    rawMessage: Buffer | null,
  ): Promise<void>;
}

function toPrismaJsonValue(value: unknown): Prisma.InputJsonValue | null {
  if (value === null) return null;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item) => toPrismaJsonValue(item));
  }
  if (typeof value === "object") {
    const result: Record<string, Prisma.InputJsonValue | null> = {};
    for (const [key, nestedValue] of Object.entries(value)) {
      result[key] = toPrismaJsonValue(nestedValue);
    }
    return result;
  }
  throw new Error(
    "Kafka event contains a value that cannot be represented as JSON",
  );
}

function toPrismaJsonObject(value: object): Prisma.InputJsonObject {
  const result: Record<string, Prisma.InputJsonValue | null> = {};
  for (const [key, nestedValue] of Object.entries(value)) {
    result[key] = toPrismaJsonValue(nestedValue);
  }
  return result;
}

function toNullablePrismaJson(
  value: unknown,
): Prisma.InputJsonValue | typeof Prisma.JsonNull {
  const jsonValue = toPrismaJsonValue(value);
  return jsonValue === null ? Prisma.JsonNull : jsonValue;
}

function serializeHeaderValue(
  value: Exclude<KafkaHeaderValue, undefined>,
): Prisma.InputJsonValue {
  if (Buffer.isBuffer(value)) {
    return { encoding: "base64", value: value.toString("base64") };
  }
  if (Array.isArray(value)) {
    return value.map((item) =>
      Buffer.isBuffer(item)
        ? { encoding: "base64", value: item.toString("base64") }
        : item,
    );
  }
  return value;
}

function serializeKafkaHeaders(
  headers: KafkaHeaders | undefined,
): Prisma.InputJsonObject | undefined {
  if (!headers) return undefined;
  const result: Record<string, Prisma.InputJsonValue> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (value !== undefined) result[key] = serializeHeaderValue(value);
  }
  return result;
}

function firstHeaderText(value: KafkaHeaderValue): string | undefined {
  const firstValue = Array.isArray(value) ? value[0] : value;
  if (typeof firstValue === "string") return firstValue;
  if (Buffer.isBuffer(firstValue)) return firstValue.toString("utf8");
  return undefined;
}

function usableCorrelationValue(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 && value.length <= 100
    ? value
    : null;
}

function correlationIds(
  event: ValidatedAuditEvent,
  headers: KafkaHeaders | undefined,
): { requestId: string | null; traceId: string | null } {
  const eventExtensions = event as Record<string, unknown>;
  const traceparent = firstHeaderText(headers?.traceparent);
  const traceparentId = traceparent?.match(
    /^[\da-f]{2}-([\da-f]{32})-[\da-f]{16}-[\da-f]{2}$/i,
  )?.[1];

  return {
    requestId:
      usableCorrelationValue(firstHeaderText(headers?.["x-request-id"])) ??
      usableCorrelationValue(eventExtensions.requestId),
    traceId:
      usableCorrelationValue(firstHeaderText(headers?.["x-trace-id"])) ??
      usableCorrelationValue(eventExtensions.traceId) ??
      usableCorrelationValue(traceparentId),
  };
}

function auditValues(event: ValidatedAuditEvent): {
  oldValue: unknown;
  newValue: unknown;
} {
  const metadata = event.metadata as Record<string, unknown>;
  if (event.eventType === "CaseStatusChanged") {
    return {
      oldValue: Object.hasOwn(metadata, "previousStatus")
        ? metadata.previousStatus
        : undefined,
      newValue: metadata.newStatus,
    };
  }
  if (event.eventType === "CaseAssigned") {
    return {
      oldValue: Object.hasOwn(metadata, "previousAssignee")
        ? metadata.previousAssignee
        : undefined,
      newValue: metadata.newAssignee,
    };
  }
  return { oldValue: undefined, newValue: undefined };
}

function isUniqueViolation(error: unknown, targetFragment: string): boolean {
  if (
    !(error instanceof Prisma.PrismaClientKnownRequestError) ||
    error.code !== "P2002"
  ) {
    return false;
  }
  const target = error.meta?.target;
  const targetText = Array.isArray(target)
    ? target.join(",")
    : String(target ?? "");
  return targetText.includes(targetFragment);
}

function isKafkaCoordinateUniqueViolation(error: unknown): boolean {
  if (
    !(error instanceof Prisma.PrismaClientKnownRequestError) ||
    error.code !== "P2002"
  ) {
    return false;
  }
  const target = error.meta?.target;
  const targetText = Array.isArray(target)
    ? target.join(",")
    : String(target ?? "");
  return (
    targetText.includes("audit_event_processing_kafka_coordinates_key") ||
    ["topic", "partition", "offset"].every((field) =>
      targetText.includes(field),
    )
  );
}

function mergeErrorMessage(
  previous: string | null,
  next: string | null,
): string | null {
  const truncatedNext = next?.slice(0, 2000) ?? null;
  if (!truncatedNext || previous?.includes(truncatedNext)) return previous;
  if (!previous) return truncatedNext;
  return `${previous}\n---\n${truncatedNext}`.slice(0, 4000);
}

export function createAuditRepository(prisma: PrismaClient): AuditRepository {
  async function upsertProcessing(
    transaction: Prisma.TransactionClient,
    coordinates: KafkaCoordinates,
    eventId: string | null,
    processingStatus: AuditProcessingOutcome,
    errorMessage: string | null,
    rawMessage?: Buffer | null,
  ): Promise<AuditProcessingOutcome> {
    const uniqueKey = {
      topic_partition_offset: {
        topic: coordinates.topic,
        partition: coordinates.partition,
        offset: BigInt(coordinates.offset),
      },
    };
    const existing = await transaction.auditEventProcessing.findUnique({
      where: uniqueKey,
    });
    const processedAt = new Date();
    const serializedHeaders = serializeKafkaHeaders(coordinates.headers);

    if (!existing) {
      await transaction.auditEventProcessing.create({
        data: {
          topic: coordinates.topic,
          partition: coordinates.partition,
          offset: BigInt(coordinates.offset),
          eventId,
          processingStatus,
          attemptCount: 1,
          errorMessage: mergeErrorMessage(null, errorMessage),
          ...(rawMessage !== undefined ? { rawMessage } : {}),
          ...(serializedHeaders ? { kafkaHeaders: serializedHeaders } : {}),
          processedAt,
        },
      });
      return processingStatus;
    }

    const alreadySuccessful =
      existing.processingStatus === "PROCESSED" ||
      existing.processingStatus === "DUPLICATE";
    const effectiveStatus = alreadySuccessful
      ? (existing.processingStatus as AuditProcessingOutcome)
      : processingStatus;

    await transaction.auditEventProcessing.update({
      where: { id: existing.id },
      data: {
        eventId: existing.eventId ?? eventId,
        processingStatus: effectiveStatus,
        attemptCount: { increment: 1 },
        errorMessage: mergeErrorMessage(existing.errorMessage, errorMessage),
        ...(existing.rawMessage === null && rawMessage !== undefined
          ? { rawMessage }
          : {}),
        ...(existing.kafkaHeaders === null && serializedHeaders
          ? { kafkaHeaders: serializedHeaders }
          : {}),
        processedAt: alreadySuccessful ? existing.processedAt : processedAt,
      },
    });
    return effectiveStatus;
  }

  async function recordProcessing(
    coordinates: KafkaCoordinates,
    eventId: string | null,
    status: AuditProcessingOutcome,
    errorMessage: string | null,
    rawMessage?: Buffer | null,
  ): Promise<AuditProcessingOutcome> {
    return prisma.$transaction((transaction) =>
      upsertProcessing(
        transaction,
        coordinates,
        eventId,
        status,
        errorMessage,
        rawMessage,
      ),
    );
  }

  return {
    async persistEvent(coordinates, event) {
      const { requestId, traceId } = correlationIds(event, coordinates.headers);
      const { oldValue, newValue } = auditValues(event);

      try {
        return await prisma.$transaction(async (transaction) => {
          const priorProcessing =
            await transaction.auditEventProcessing.findUnique({
              where: {
                topic_partition_offset: {
                  topic: coordinates.topic,
                  partition: coordinates.partition,
                  offset: BigInt(coordinates.offset),
                },
              },
              select: { processingStatus: true },
            });
          if (
            priorProcessing?.processingStatus === "PROCESSED" ||
            priorProcessing?.processingStatus === "DUPLICATE"
          ) {
            return upsertProcessing(
              transaction,
              coordinates,
              event.eventId,
              priorProcessing.processingStatus as AuditProcessingOutcome,
              null,
            );
          }

          const existingEvent = await transaction.auditLog.findUnique({
            where: { eventId: event.eventId },
            select: { id: true },
          });
          if (existingEvent) {
            return upsertProcessing(
              transaction,
              coordinates,
              event.eventId,
              "DUPLICATE",
              null,
            );
          }

          await transaction.auditLog.create({
            data: {
              eventId: event.eventId,
              tenantId: event.tenantId,
              actorId: event.actorId ?? null,
              action: event.eventType,
              eventVersion: event.eventVersion,
              resourceType: event.resourceType,
              resourceId: event.resourceId,
              timestamp: new Date(event.timestamp),
              payload: toPrismaJsonObject(event),
              ...(oldValue !== undefined
                ? { oldValue: toNullablePrismaJson(oldValue) }
                : {}),
              ...(newValue !== undefined
                ? { newValue: toNullablePrismaJson(newValue) }
                : {}),
              requestId,
              traceId,
            },
          });
          return upsertProcessing(
            transaction,
            coordinates,
            event.eventId,
            "PROCESSED",
            null,
          );
        });
      } catch (error) {
        if (isUniqueViolation(error, "event_id")) {
          await recordProcessing(coordinates, event.eventId, "DUPLICATE", null);
          return "DUPLICATE";
        }
        if (isKafkaCoordinateUniqueViolation(error)) {
          return recordProcessing(
            coordinates,
            event.eventId,
            "PROCESSED",
            null,
          );
        }
        throw error;
      }
    },

    async recordInvalid(coordinates, eventId, errorMessage, rawMessage) {
      await recordProcessing(
        coordinates,
        eventId,
        "INVALID",
        errorMessage,
        rawMessage,
      );
    },
  };
}
