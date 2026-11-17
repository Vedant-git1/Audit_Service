import { randomUUID } from 'node:crypto';
import {
  auditEventTopics,
  type AuditEventType,
} from '../../src/modules/audit/audit-event.schema.js';

export interface DummyEvent {
  eventId: string;
  eventType: AuditEventType;
  eventVersion: number;
  tenantId: string;
  actorId: string | null;
  resourceType: 'CASE' | 'TASK' | 'DOCUMENT' | 'APPROVAL';
  resourceId: string;
  timestamp: string;
  metadata: Record<string, string | null>;
}

export interface DummyKafkaRecord {
  topic: string;
  key: string;
  value: string;
  event: DummyEvent;
}

export const dummyEventTypes = Object.keys(
  auditEventTopics,
) as AuditEventType[];

interface RelatedResources {
  tenantId: string;
  actorId: string;
  assigneeId: string;
  teamId: string;
  caseId: string;
  taskId: string;
  documentId: string;
  approvalId: string;
}

function createRelatedResources(): RelatedResources {
  return {
    tenantId: randomUUID(),
    actorId: randomUUID(),
    assigneeId: randomUUID(),
    teamId: randomUUID(),
    caseId: randomUUID(),
    taskId: randomUUID(),
    documentId: randomUUID(),
    approvalId: randomUUID(),
  };
}

function createEvent(
  eventType: AuditEventType,
  resources: RelatedResources,
  timestamp: string,
): DummyEvent {
  const common = {
    eventId: randomUUID(),
    eventType,
    eventVersion: 1,
    tenantId: resources.tenantId,
    actorId: eventType === 'TaskCreated' ? null : resources.actorId,
    timestamp,
  };

  switch (eventType) {
    case 'CaseCreated':
      return {
        ...common,
        resourceType: 'CASE',
        resourceId: resources.caseId,
        metadata: { caseId: resources.caseId },
      };
    case 'CaseAssigned':
      return {
        ...common,
        resourceType: 'CASE',
        resourceId: resources.caseId,
        metadata: {
          caseId: resources.caseId,
          previousAssignee: null,
          newAssignee: resources.assigneeId,
        },
      };
    case 'CaseStatusChanged':
      return {
        ...common,
        resourceType: 'CASE',
        resourceId: resources.caseId,
        metadata: {
          caseId: resources.caseId,
          previousStatus: 'OPEN',
          newStatus: 'ASSIGNED',
        },
      };
    case 'CaseResolved':
      return {
        ...common,
        resourceType: 'CASE',
        resourceId: resources.caseId,
        metadata: { caseId: resources.caseId },
      };
    case 'TaskCreated':
      return {
        ...common,
        resourceType: 'TASK',
        resourceId: resources.taskId,
        metadata: {
          taskId: resources.taskId,
          caseId: resources.caseId,
          assignedToUserId: resources.assigneeId,
          assignedToTeamId: resources.teamId,
        },
      };
    case 'TaskCompleted':
      return {
        ...common,
        resourceType: 'TASK',
        resourceId: resources.taskId,
        metadata: {
          taskId: resources.taskId,
          caseId: resources.caseId,
          completedAt: timestamp,
        },
      };
    case 'DocumentUploaded':
      return {
        ...common,
        resourceType: 'DOCUMENT',
        resourceId: resources.documentId,
        metadata: {
          documentId: resources.documentId,
          caseId: resources.caseId,
          storageKey: `documents/${resources.tenantId}/${resources.caseId}/${resources.documentId}/sample.pdf`,
        },
      };
    case 'DocumentScanned':
      return {
        ...common,
        resourceType: 'DOCUMENT',
        resourceId: resources.documentId,
        metadata: {
          documentId: resources.documentId,
          caseId: resources.caseId,
          scanStatus: 'CLEAN',
        },
      };
    case 'ApprovalRequested':
      return {
        ...common,
        resourceType: 'APPROVAL',
        resourceId: resources.approvalId,
        metadata: {
          approvalId: resources.approvalId,
          caseId: resources.caseId,
          approverId: resources.assigneeId,
        },
      };
    case 'ApprovalCompleted':
      return {
        ...common,
        resourceType: 'APPROVAL',
        resourceId: resources.approvalId,
        metadata: {
          approvalId: resources.approvalId,
          caseId: resources.caseId,
          decision: 'APPROVED',
        },
      };
  }
}

export function createDummyEvents(
  count: number,
  eventType?: AuditEventType,
): DummyKafkaRecord[] {
  if (!Number.isInteger(count) || count < 1 || count > 100_000) {
    throw new Error('Event count must be an integer between 1 and 100000');
  }

  const relatedByGroup = new Map<number, RelatedResources>();
  return Array.from({ length: count }, (_, index) => {
    const type = eventType ?? dummyEventTypes[index % dummyEventTypes.length]!;
    const relatedGroup = eventType
      ? index
      : Math.floor(index / dummyEventTypes.length);
    const resources =
      relatedByGroup.get(relatedGroup) ?? createRelatedResources();
    relatedByGroup.set(relatedGroup, resources);
    const timestamp = new Date(Date.now() + index).toISOString();
    const event = createEvent(type, resources, timestamp);

    return {
      topic: auditEventTopics[type],
      key: event.resourceId,
      value: JSON.stringify(event),
      event,
    };
  });
}

export function createDuplicateEvents(
  eventType: AuditEventType,
  count: number,
): DummyKafkaRecord[] {
  if (!Number.isInteger(count) || count < 2 || count > 100_000) {
    throw new Error('Duplicate count must be an integer between 2 and 100000');
  }
  const [record] = createDummyEvents(1, eventType);
  if (!record) throw new Error('Failed to generate duplicate event');
  return Array.from({ length: count }, () => ({ ...record }));
}

export function createInvalidEvent(
  scenario: 'invalid-json' | 'invalid-envelope' | 'unsupported-version',
  eventType: AuditEventType = 'CaseCreated',
): DummyKafkaRecord {
  const [record] = createDummyEvents(1, eventType);
  if (!record) throw new Error('Failed to generate invalid test event');

  if (scenario === 'invalid-json') {
    return { ...record, value: '{"eventId":' };
  }

  const event = { ...record.event };
  if (scenario === 'invalid-envelope') {
    const invalidEvent: Record<string, unknown> = { ...event };
    delete invalidEvent.resourceId;
    return { ...record, value: JSON.stringify(invalidEvent) };
  }
  event.eventVersion = 2;
  return { ...record, event, value: JSON.stringify(event) };
}
