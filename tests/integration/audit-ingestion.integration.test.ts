import "dotenv/config";
import { randomUUID } from "node:crypto";
import { prisma } from "../../src/infrastructure/database/prisma.js";
import { createAuditIngestionService } from "../../src/modules/audit/audit-ingestion.service.js";
import { createAuditRepository } from "../../src/modules/audit/audit.repository.js";

const enabled = process.env.RUN_DB_TESTS === "true";
const suite = enabled ? describe : describe.skip;

let offsetSequence = 0;
const nextOffset = () => `${Date.now()}${++offsetSequence}`;

function caseEvent(
  eventType: string,
  metadata: Record<string, unknown>,
  actorId: string | null = null,
) {
  return {
    eventId: randomUUID(),
    eventType,
    eventVersion: 1,
    tenantId: "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    actorId,
    resourceType: "CASE",
    resourceId: "c1a2b3d4-0000-4000-8000-000000000042",
    timestamp: new Date().toISOString(),
    metadata,
  };
}

function coordinates(offset = nextOffset()) {
  return {
    topic: "resolve.case.created",
    partition: 0,
    offset,
  };
}

async function ingest(
  event: Record<string, unknown>,
  messageCoordinates = coordinates(),
  headers?: Record<string, string | Buffer>,
) {
  return createAuditIngestionService(
    createAuditRepository(prisma),
  ).processMessage({
    ...messageCoordinates,
    value: Buffer.from(JSON.stringify(event)),
    ...(headers ? { headers } : {}),
  });
}

suite("PostgreSQL audit ingestion", () => {
  beforeAll(async () => {
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
  });

  it("persists CaseCreated with canonical fields, nullable actor, payload, and correlation IDs", async () => {
    const event = caseEvent("CaseCreated", {
      caseId: "c1a2b3d4-0000-4000-8000-000000000042",
    });
    const messageCoordinates = coordinates();
    const outcome = await ingest(event, messageCoordinates, {
      "x-request-id": "request-123",
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    });

    expect(outcome).toBe("PROCESSED");
    const stored = await prisma.auditLog.findUnique({
      where: { eventId: event.eventId as string },
    });
    expect(stored).toMatchObject({
      action: "CaseCreated",
      tenantId: event.tenantId,
      actorId: null,
      resourceType: "CASE",
      requestId: "request-123",
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
      oldValue: null,
      newValue: null,
    });
    expect(stored?.timestamp.toISOString()).toBe(event.timestamp);
    expect(stored?.payload).toEqual(event);
    const processingRecord = await prisma.auditEventProcessing.findUnique({
      where: {
        topic_partition_offset: {
          topic: messageCoordinates.topic,
          partition: messageCoordinates.partition,
          offset: BigInt(messageCoordinates.offset),
        },
      },
    });
    expect(processingRecord?.processingStatus).toBe("PROCESSED");
  });

  it("maps CaseStatusChanged metadata to old and new audit values", async () => {
    const event = caseEvent("CaseStatusChanged", {
      caseId: "c1a2b3d4-0000-4000-8000-000000000042",
      previousStatus: "REVIEW",
      newStatus: "REJECTED",
    });

    await expect(
      ingest(event, {
        ...coordinates(),
        topic: "resolve.case.statuschanged",
      }),
    ).resolves.toBe("PROCESSED");

    const stored = await prisma.auditLog.findUnique({
      where: { eventId: event.eventId as string },
    });
    expect(stored?.action).toBe("CaseStatusChanged");
    expect(stored?.oldValue).toBe("REVIEW");
    expect(stored?.newValue).toBe("REJECTED");
  });

  it("maps CaseAssigned assignees to old and new audit values", async () => {
    const event = caseEvent(
      "CaseAssigned",
      {
        caseId: "c1a2b3d4-0000-4000-8000-000000000042",
        previousAssignee: "9e1071ea-9a3b-4a2f-9e2e-1a2b3c4d5e6f",
        newAssignee: "9e1071ea-9a3b-4a2f-9e2e-1a2b3c4d5e70",
      },
      "9e1071ea-9a3b-4a2f-9e2e-1a2b3c4d5e6f",
    );

    await expect(
      ingest(event, {
        ...coordinates(),
        topic: "resolve.case.assigned",
      }),
    ).resolves.toBe("PROCESSED");

    const stored = await prisma.auditLog.findUnique({
      where: { eventId: event.eventId as string },
    });
    expect(stored?.oldValue).toBe("9e1071ea-9a3b-4a2f-9e2e-1a2b3c4d5e6f");
    expect(stored?.newValue).toBe("9e1071ea-9a3b-4a2f-9e2e-1a2b3c4d5e70");
  });

  it("leaves old and new values unset when the source event has no before/after values", async () => {
    const event = caseEvent("CaseCreated", {
      caseId: "c1a2b3d4-0000-4000-8000-000000000042",
    });

    await ingest(event);

    const stored = await prisma.auditLog.findUnique({
      where: { eventId: event.eventId as string },
    });
    expect(stored?.oldValue).toBeNull();
    expect(stored?.newValue).toBeNull();
  });

  it("deduplicates the same eventId delivered at different Kafka offsets", async () => {
    const event = caseEvent("CaseCreated", {
      caseId: "c1a2b3d4-0000-4000-8000-000000000042",
    });
    const firstCoordinates = coordinates();

    await expect(ingest(event, firstCoordinates)).resolves.toBe("PROCESSED");
    const duplicateCoordinates = {
      ...firstCoordinates,
      offset: nextOffset(),
    };
    await expect(ingest(event, duplicateCoordinates)).resolves.toBe(
      "DUPLICATE",
    );

    expect(
      await prisma.auditLog.count({
        where: { eventId: event.eventId as string },
      }),
    ).toBe(1);
    const duplicateTracker = await prisma.auditEventProcessing.findUnique({
      where: {
        topic_partition_offset: {
          topic: duplicateCoordinates.topic,
          partition: duplicateCoordinates.partition,
          offset: BigInt(duplicateCoordinates.offset),
        },
      },
    });
    expect(duplicateTracker?.processingStatus).toBe("DUPLICATE");
  });

  it("keeps processing status stable when the same Kafka coordinates are replayed", async () => {
    const event = caseEvent("CaseCreated", {
      caseId: "c1a2b3d4-0000-4000-8000-000000000042",
    });
    const messageCoordinates = coordinates();

    await expect(ingest(event, messageCoordinates)).resolves.toBe("PROCESSED");
    await expect(ingest(event, messageCoordinates)).resolves.toBe("PROCESSED");

    const tracker = await prisma.auditEventProcessing.findUnique({
      where: {
        topic_partition_offset: {
          topic: messageCoordinates.topic,
          partition: messageCoordinates.partition,
          offset: BigInt(messageCoordinates.offset),
        },
      },
    });
    expect(tracker?.processingStatus).toBe("PROCESSED");
    expect(tracker?.attemptCount).toBe(2);
    expect(
      await prisma.auditLog.count({
        where: { eventId: event.eventId as string },
      }),
    ).toBe(1);
  });

  it("preserves invalid raw bytes and Kafka headers for malformed JSON", async () => {
    const messageCoordinates = coordinates();
    const rawMessage = Buffer.from("{not-json");
    const headers = {
      "x-request-id": "invalid-request",
      "binary-header": Buffer.from([0, 255, 17]),
    };
    const service = createAuditIngestionService(createAuditRepository(prisma));

    await expect(
      service.processMessage({
        ...messageCoordinates,
        value: rawMessage,
        headers,
      }),
    ).resolves.toBe("INVALID");

    const tracker = await prisma.auditEventProcessing.findUnique({
      where: {
        topic_partition_offset: {
          topic: messageCoordinates.topic,
          partition: messageCoordinates.partition,
          offset: BigInt(messageCoordinates.offset),
        },
      },
    });
    expect(Buffer.from(tracker?.rawMessage ?? [])).toEqual(rawMessage);
    expect(tracker?.kafkaHeaders).toEqual({
      "x-request-id": "invalid-request",
      "binary-header": { encoding: "base64", value: "AP8R" },
    });
    expect(tracker?.errorMessage).toContain("Message value is not valid JSON");
    expect(tracker?.processedAt).toBeInstanceOf(Date);
  });

  it("rolls back the audit row when the processing record cannot be written", async () => {
    const event = caseEvent("CaseCreated", {
      caseId: "c1a2b3d4-0000-4000-8000-000000000042",
    });
    const messageCoordinates = coordinates();
    const suffix = randomUUID().replaceAll("-", "");
    const functionName = `audit_test_reject_processing_${suffix}`;
    const triggerName = `audit_test_reject_processing_${suffix}`;

    try {
      await prisma.$executeRawUnsafe(
        `CREATE FUNCTION "${functionName}"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced processing failure'; END; $$`,
      );
      await prisma.$executeRawUnsafe(
        `CREATE TRIGGER "${triggerName}" BEFORE INSERT ON "audit_event_processing" FOR EACH ROW EXECUTE FUNCTION "${functionName}"()`,
      );
      await expect(ingest(event, messageCoordinates)).rejects.toThrow(
        "forced processing failure",
      );
      expect(
        await prisma.auditLog.findUnique({
          where: { eventId: event.eventId as string },
        }),
      ).toBeNull();
      expect(
        await prisma.auditEventProcessing.findUnique({
          where: {
            topic_partition_offset: {
              topic: messageCoordinates.topic,
              partition: messageCoordinates.partition,
              offset: BigInt(messageCoordinates.offset),
            },
          },
        }),
      ).toBeNull();
    } finally {
      await prisma.$executeRawUnsafe(
        `DROP TRIGGER IF EXISTS "${triggerName}" ON "audit_event_processing"`,
      );
      await prisma.$executeRawUnsafe(
        `DROP FUNCTION IF EXISTS "${functionName}"()`,
      );
    }
  });
});
