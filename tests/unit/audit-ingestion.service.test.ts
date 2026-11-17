import { auditEventTopics } from "../../src/modules/audit/audit-event.schema.js";
import {
  createAuditIngestionService,
  type KafkaAuditMessage,
} from "../../src/modules/audit/audit-ingestion.service.js";
import type { AuditRepository } from "../../src/modules/audit/audit.repository.js";

const event = {
  eventId: "b2e1a4c0-1111-4a2b-9c3d-000000000010",
  eventType: "CaseCreated",
  eventVersion: 1,
  tenantId: "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  actorId: "9e1071ea-9a3b-4a2f-9e2e-1a2b3c4d5e6f",
  resourceType: "CASE",
  resourceId: "c1a2b3d4-0000-4000-8000-000000000042",
  timestamp: "2026-09-08T10:00:00Z",
  metadata: { caseId: "c1a2b3d4-0000-4000-8000-000000000042" },
};

function repositoryMock(): jest.Mocked<AuditRepository> {
  return {
    persistEvent: jest.fn().mockResolvedValue("PROCESSED"),
    recordInvalid: jest.fn().mockResolvedValue(undefined),
  };
}

function message(value: Buffer | null, offset = "42"): KafkaAuditMessage {
  return {
    topic: auditEventTopics.CaseCreated,
    partition: 2,
    offset,
    value,
  };
}

describe("Audit ingestion service", () => {
  it("persists a validated event through the repository", async () => {
    const repository = repositoryMock();
    const service = createAuditIngestionService(repository);

    await expect(
      service.processMessage(message(Buffer.from(JSON.stringify(event)))),
    ).resolves.toBe("PROCESSED");
    expect(repository.persistEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: "resolve.case.created",
        partition: 2,
        offset: "42",
      }),
      expect.objectContaining({ eventId: event.eventId }),
    );
  });

  it("records duplicate event IDs as successfully handled", async () => {
    const repository = repositoryMock();
    repository.persistEvent.mockResolvedValue("DUPLICATE");
    const service = createAuditIngestionService(repository);

    await expect(
      service.processMessage(message(Buffer.from(JSON.stringify(event)))),
    ).resolves.toBe("DUPLICATE");
  });

  it("records invalid JSON and missing fields as terminal INVALID outcomes", async () => {
    const repository = repositoryMock();
    const service = createAuditIngestionService(repository);

    await expect(
      service.processMessage(message(Buffer.from("{broken"))),
    ).resolves.toBe("INVALID");
    await expect(
      service.processMessage(
        message(
          Buffer.from(
            JSON.stringify({
              eventId: event.eventId,
              eventType: "CaseCreated",
            }),
          ),
        ),
      ),
    ).resolves.toBe("INVALID");

    expect(repository.recordInvalid).toHaveBeenCalledTimes(2);
    expect(repository.recordInvalid).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        topic: "resolve.case.created",
        partition: 2,
        offset: "42",
      }),
      null,
      "Message value is not valid JSON",
      Buffer.from("{broken"),
    );
  });

  it("quarantines unsupported event versions with raw payload and failure reason", async () => {
    const repository = repositoryMock();
    const service = createAuditIngestionService(repository);
    const unsupportedEvent = { ...event, eventVersion: 2 };
    const rawMessage = Buffer.from(JSON.stringify(unsupportedEvent));

    await expect(
      service.processMessage(message(rawMessage, "43")),
    ).resolves.toBe("INVALID");
    expect(repository.recordInvalid).toHaveBeenCalledWith(
      expect.objectContaining({ offset: "43" }),
      event.eventId,
      "Unsupported CaseCreated version: 2",
      rawMessage,
    );
    expect(repository.persistEvent).not.toHaveBeenCalled();
  });

  it("passes Kafka headers to persistence for correlation mapping", async () => {
    const repository = repositoryMock();
    const service = createAuditIngestionService(repository);
    const headers = {
      "x-request-id": "req-123",
      "x-trace-id": "4bf92f3577b34da6a3ce929d0e0e4736",
    };

    await service.processMessage({
      ...message(Buffer.from(JSON.stringify(event))),
      headers,
    });

    expect(repository.persistEvent).toHaveBeenCalledWith(
      expect.objectContaining({ headers }),
      expect.objectContaining({ eventId: event.eventId }),
    );
  });

  it("does not convert database failures into handled messages", async () => {
    const repository = repositoryMock();
    repository.persistEvent.mockRejectedValue(
      new Error("database unavailable"),
    );
    const service = createAuditIngestionService(repository);

    await expect(
      service.processMessage(message(Buffer.from(JSON.stringify(event)))),
    ).rejects.toThrow("database unavailable");
    expect(repository.recordInvalid).not.toHaveBeenCalled();
  });
});
