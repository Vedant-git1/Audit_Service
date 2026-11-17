import {
  auditEventTopics,
  getAuditEventTypes,
  InvalidAuditEventError,
  parseJsonEvent,
  validateAuditEvent,
  type AuditEventType,
} from "../../src/modules/audit/audit-event.schema.js";

const ids = {
  event: "b2e1a4c0-1111-4a2b-9c3d-000000000010",
  tenant: "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  actor: "9e1071ea-9a3b-4a2f-9e2e-1a2b3c4d5e6f",
  case: "c1a2b3d4-0000-4000-8000-000000000042",
  task: "d1e2f3a4-0000-4000-8000-000000000099",
  document: "e1f2a3b4-0000-4000-8000-000000000077",
  approval: "f1a2b3c4-0000-4000-8000-000000000055",
};

const eventCases: Array<{
  type: AuditEventType;
  resourceType: string;
  resourceId: string;
  metadata: Record<string, unknown>;
}> = [
  {
    type: "CaseCreated",
    resourceType: "CASE",
    resourceId: ids.case,
    metadata: { caseId: ids.case },
  },
  {
    type: "CaseAssigned",
    resourceType: "CASE",
    resourceId: ids.case,
    metadata: {
      caseId: ids.case,
      previousAssignee: null,
      newAssignee: ids.actor,
    },
  },
  {
    type: "CaseStatusChanged",
    resourceType: "CASE",
    resourceId: ids.case,
    metadata: {
      caseId: ids.case,
      previousStatus: "REVIEW",
      newStatus: "REJECTED",
    },
  },
  {
    type: "CaseResolved",
    resourceType: "CASE",
    resourceId: ids.case,
    metadata: { caseId: ids.case },
  },
  {
    type: "TaskCreated",
    resourceType: "TASK",
    resourceId: ids.task,
    metadata: {
      taskId: ids.task,
      caseId: ids.case,
      assignedToUserId: ids.actor,
      assignedToTeamId: null,
    },
  },
  {
    type: "TaskCompleted",
    resourceType: "TASK",
    resourceId: ids.task,
    metadata: {
      taskId: ids.task,
      caseId: ids.case,
      completedAt: "2026-09-08T14:00:00Z",
    },
  },
  {
    type: "DocumentUploaded",
    resourceType: "DOCUMENT",
    resourceId: ids.document,
    metadata: {
      documentId: ids.document,
      caseId: ids.case,
      storageKey: "documents/example.pdf",
    },
  },
  {
    type: "DocumentScanned",
    resourceType: "DOCUMENT",
    resourceId: ids.document,
    metadata: {
      documentId: ids.document,
      caseId: ids.case,
      scanStatus: "CLEAN",
    },
  },
  {
    type: "ApprovalRequested",
    resourceType: "APPROVAL",
    resourceId: ids.approval,
    metadata: { approvalId: ids.approval, caseId: ids.case, approverId: null },
  },
  {
    type: "ApprovalCompleted",
    resourceType: "APPROVAL",
    resourceId: ids.approval,
    metadata: {
      approvalId: ids.approval,
      caseId: ids.case,
      decision: "APPROVED",
    },
  },
];

function createEvent(
  type: AuditEventType,
  resourceType = "CASE",
  resourceId = ids.case,
  metadata: Record<string, unknown> = { caseId: ids.case },
) {
  return {
    eventId: ids.event,
    eventType: type,
    eventVersion: 1,
    tenantId: ids.tenant,
    actorId: ids.actor,
    resourceType,
    resourceId,
    timestamp: "2026-09-08T10:00:00Z",
    metadata,
  };
}

describe("Resolve Kafka event contracts", () => {
  it("accepts a valid CaseCreated envelope and preserves unknown payload fields", () => {
    const event = {
      ...createEvent("CaseCreated"),
      producerExtension: { retained: true },
      metadata: {
        caseId: ids.case,
        producerMetadataExtension: { retained: true },
      },
    };

    expect(validateAuditEvent(auditEventTopics.CaseCreated, event)).toEqual(
      event,
    );
  });

  it("accepts a valid CaseAssigned event with a nullable previous assignee", () => {
    const event = createEvent("CaseAssigned", "CASE", ids.case, {
      caseId: ids.case,
      previousAssignee: null,
      newAssignee: ids.actor,
    });

    expect(
      validateAuditEvent(auditEventTopics.CaseAssigned, event).metadata,
    ).toEqual(event.metadata);
  });

  it.each(eventCases)(
    "validates $type on its documented topic",
    (eventCase) => {
      const event = createEvent(
        eventCase.type,
        eventCase.resourceType,
        eventCase.resourceId,
        eventCase.metadata,
      );

      expect(
        validateAuditEvent(auditEventTopics[eventCase.type], event).eventType,
      ).toBe(eventCase.type);
    },
  );

  it("declares exactly the ten documented topic subscriptions", () => {
    expect(getAuditEventTypes()).toHaveLength(10);
    expect(Object.values(auditEventTopics)).toEqual([
      "resolve.case.created",
      "resolve.case.assigned",
      "resolve.case.statuschanged",
      "resolve.case.resolved",
      "resolve.task.created",
      "resolve.task.completed",
      "resolve.document.uploaded",
      "resolve.document.scanned",
      "resolve.approval.requested",
      "resolve.approval.completed",
    ]);
  });

  it("classifies invalid JSON as terminal validation failure", () => {
    try {
      parseJsonEvent("{invalid");
      throw new Error("Expected invalid JSON to be rejected");
    } catch (error) {
      expect(error).toBeInstanceOf(InvalidAuditEventError);
      expect((error as InvalidAuditEventError).reason).toBe("INVALID_JSON");
    }
  });

  it("rejects missing envelope fields and invalid UUIDs", () => {
    const missingField = createEvent("CaseCreated") as Record<string, unknown>;
    delete missingField.tenantId;
    expect(() =>
      validateAuditEvent(auditEventTopics.CaseCreated, missingField),
    ).toThrow(expect.objectContaining({ reason: "INVALID_ENVELOPE" }));

    const invalidUuid = {
      ...createEvent("CaseCreated"),
      eventId: "not-a-uuid",
    };
    expect(() =>
      validateAuditEvent(auditEventTopics.CaseCreated, invalidUuid),
    ).toThrow(expect.objectContaining({ reason: "INVALID_ENVELOPE" }));
  });

  it("rejects invalid event-specific metadata", () => {
    const event = createEvent("CaseAssigned", "CASE", ids.case, {
      caseId: "not-a-uuid",
      newAssignee: ids.actor,
    });

    expect(() =>
      validateAuditEvent(auditEventTopics.CaseAssigned, event),
    ).toThrow(expect.objectContaining({ reason: "INVALID_METADATA" }));
  });

  it("rejects unknown event types, topic mismatches, and unsupported versions", () => {
    expect(() =>
      validateAuditEvent("resolve.case.created", {
        ...createEvent("CaseCreated"),
        eventType: "CaseArchived",
      }),
    ).toThrow(expect.objectContaining({ reason: "UNKNOWN_EVENT_TYPE" }));

    expect(() =>
      validateAuditEvent(auditEventTopics.CaseCreated, {
        ...createEvent("CaseCreated"),
        eventVersion: 2,
      }),
    ).toThrow(expect.objectContaining({ reason: "UNSUPPORTED_EVENT_VERSION" }));

    expect(() =>
      validateAuditEvent(
        auditEventTopics.CaseResolved,
        createEvent("CaseCreated"),
      ),
    ).toThrow(expect.objectContaining({ reason: "TOPIC_EVENT_TYPE_MISMATCH" }));
  });

  it("allows actorId to be absent or null as specified by the contract", () => {
    const absentActor = createEvent("TaskCreated", "TASK", ids.task, {
      taskId: ids.task,
      caseId: ids.case,
    }) as Record<string, unknown>;
    delete absentActor.actorId;

    expect(
      validateAuditEvent(auditEventTopics.TaskCreated, absentActor).actorId,
    ).toBeUndefined();
    expect(
      validateAuditEvent(auditEventTopics.DocumentScanned, {
        ...createEvent("DocumentScanned", "DOCUMENT", ids.document, {
          documentId: ids.document,
          caseId: ids.case,
          scanStatus: "CLEAN",
        }),
        actorId: null,
      }).actorId,
    ).toBeNull();
  });
});
