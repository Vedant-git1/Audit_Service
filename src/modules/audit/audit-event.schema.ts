import { z } from "zod";

const uuid = z.string().uuid();
const nullableUuid = uuid.nullable();
const status = z.enum([
  "OPEN",
  "ASSIGNED",
  "INVESTIGATION",
  "REVIEW",
  "APPROVED",
  "REJECTED",
  "RESOLVED",
  "REOPENED",
]);

const metadataSchemas = {
  CaseCreated: z.object({ caseId: uuid }),
  CaseAssigned: z.object({
    caseId: uuid,
    previousAssignee: nullableUuid.optional(),
    newAssignee: uuid,
  }),
  CaseStatusChanged: z.object({
    caseId: uuid,
    previousStatus: status.nullable().optional(),
    newStatus: status,
  }),
  CaseResolved: z.object({ caseId: uuid }),
  TaskCreated: z.object({
    taskId: uuid,
    caseId: uuid,
    assignedToUserId: nullableUuid.optional(),
    assignedToTeamId: nullableUuid.optional(),
  }),
  TaskCompleted: z.object({
    taskId: uuid,
    caseId: uuid,
    completedAt: z.string().datetime({ offset: true }),
  }),
  DocumentUploaded: z.object({
    documentId: uuid,
    caseId: uuid,
    storageKey: z.string(),
  }),
  ApprovalRequested: z.object({
    approvalId: uuid,
    caseId: uuid,
    approverId: nullableUuid.optional(),
  }),
  ApprovalCompleted: z.object({
    approvalId: uuid,
    caseId: uuid,
    decision: z.enum(["APPROVED", "REJECTED", "EXPIRED"]),
  }),
  DocumentScanned: z.object({
    documentId: uuid,
    caseId: uuid,
    scanStatus: z.enum(["CLEAN", "INFECTED", "FAILED"]),
  }),
} as const;

export type AuditEventType = keyof typeof metadataSchemas;

export const auditEventTopics = {
  CaseCreated: "resolve.case.created",
  CaseAssigned: "resolve.case.assigned",
  CaseStatusChanged: "resolve.case.statuschanged",
  CaseResolved: "resolve.case.resolved",
  TaskCreated: "resolve.task.created",
  TaskCompleted: "resolve.task.completed",
  DocumentUploaded: "resolve.document.uploaded",
  DocumentScanned: "resolve.document.scanned",
  ApprovalRequested: "resolve.approval.requested",
  ApprovalCompleted: "resolve.approval.completed",
} as const satisfies Record<AuditEventType, string>;

export const auditKafkaTopics = Object.values(auditEventTopics);

const expectedResourceTypes: Record<AuditEventType, string> = {
  CaseCreated: "CASE",
  CaseAssigned: "CASE",
  CaseStatusChanged: "CASE",
  CaseResolved: "CASE",
  TaskCreated: "TASK",
  TaskCompleted: "TASK",
  DocumentUploaded: "DOCUMENT",
  DocumentScanned: "DOCUMENT",
  ApprovalRequested: "APPROVAL",
  ApprovalCompleted: "APPROVAL",
};

const envelopeSchema = z
  .object({
    eventId: uuid,
    eventType: z.string().min(1),
    eventVersion: z.number().int().positive(),
    tenantId: uuid,
    actorId: nullableUuid.optional(),
    resourceType: z.enum(["CASE", "TASK", "DOCUMENT", "APPROVAL"]),
    resourceId: uuid,
    timestamp: z.string().datetime({ offset: true }),
    metadata: z.record(z.unknown()),
  })
  .passthrough();

export type ValidatedAuditEvent = z.infer<typeof envelopeSchema> & {
  eventType: AuditEventType;
  eventVersion: 1;
};

export class InvalidAuditEventError extends Error {
  constructor(
    readonly reason:
      | "INVALID_JSON"
      | "INVALID_ENVELOPE"
      | "UNKNOWN_EVENT_TYPE"
      | "TOPIC_EVENT_TYPE_MISMATCH"
      | "UNSUPPORTED_EVENT_VERSION"
      | "RESOURCE_TYPE_MISMATCH"
      | "INVALID_METADATA",
    message: string,
  ) {
    super(message);
    this.name = "InvalidAuditEventError";
  }
}

export function parseJsonEvent(value: Buffer | string | null): unknown {
  try {
    if (value === null) {
      throw new Error("Empty Kafka message");
    }
    return JSON.parse(
      typeof value === "string" ? value : value.toString("utf8"),
    ) as unknown;
  } catch {
    throw new InvalidAuditEventError(
      "INVALID_JSON",
      "Message value is not valid JSON",
    );
  }
}

export function validateAuditEvent(
  topic: string,
  value: unknown,
): ValidatedAuditEvent {
  const envelope = envelopeSchema.safeParse(value);
  if (!envelope.success) {
    throw new InvalidAuditEventError(
      "INVALID_ENVELOPE",
      envelope.error.issues
        .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
        .join("; "),
    );
  }

  const eventType = envelope.data.eventType as AuditEventType;
  const expectedTopic = auditEventTopics[eventType];
  if (!expectedTopic) {
    throw new InvalidAuditEventError(
      "UNKNOWN_EVENT_TYPE",
      `Unsupported event type: ${envelope.data.eventType}`,
    );
  }
  if (topic !== expectedTopic) {
    throw new InvalidAuditEventError(
      "TOPIC_EVENT_TYPE_MISMATCH",
      `Event type ${eventType} is not valid on topic ${topic}`,
    );
  }
  if (envelope.data.eventVersion !== 1) {
    throw new InvalidAuditEventError(
      "UNSUPPORTED_EVENT_VERSION",
      `Unsupported ${eventType} version: ${envelope.data.eventVersion}`,
    );
  }
  if (envelope.data.resourceType !== expectedResourceTypes[eventType]) {
    throw new InvalidAuditEventError(
      "RESOURCE_TYPE_MISMATCH",
      `Resource type does not match ${eventType}`,
    );
  }

  const metadata = metadataSchemas[eventType].safeParse(envelope.data.metadata);
  if (!metadata.success) {
    throw new InvalidAuditEventError(
      "INVALID_METADATA",
      metadata.error.issues
        .map((issue) => `metadata.${issue.path.join(".")}: ${issue.message}`)
        .join("; "),
    );
  }

  return envelope.data as ValidatedAuditEvent;
}

export function extractEventId(value: unknown): string | null {
  if (typeof value !== "object" || value === null || !("eventId" in value))
    return null;
  const result = uuid.safeParse(value.eventId);
  return result.success ? result.data : null;
}

export function getAuditEventTypes(): AuditEventType[] {
  return Object.keys(auditEventTopics) as AuditEventType[];
}
