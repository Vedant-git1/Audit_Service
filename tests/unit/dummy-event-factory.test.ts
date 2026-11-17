import {
  parseJsonEvent,
  validateAuditEvent,
} from '../../src/modules/audit/audit-event.schema.js';
import {
  createDuplicateEvents,
  createDummyEvents,
  createInvalidEvent,
  dummyEventTypes,
} from '../../scripts/dummy-events/event-factory.js';

describe('dummy event factory', () => {
  it('creates valid contract events for all ten types with related resource references', () => {
    const records = createDummyEvents(10);

    expect(records).toHaveLength(10);
    expect(records.map((record) => record.event.eventType)).toEqual(
      dummyEventTypes,
    );
    records.forEach((record) => {
      expect(() =>
        validateAuditEvent(record.topic, JSON.parse(record.value)),
      ).not.toThrow();
      expect(record.event.eventId).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      );
      expect(record.event.tenantId).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      );
      expect(new Date(record.event.timestamp).toISOString()).toBe(
        record.event.timestamp,
      );
      expect(record.key).toBe(record.event.resourceId);
    });

    const eventByType = new Map(
      records.map((record) => [record.event.eventType, record.event]),
    );
    const caseId = eventByType.get('CaseCreated')?.resourceId;
    expect(eventByType.get('CaseAssigned')?.resourceId).toBe(caseId);
    expect(eventByType.get('CaseStatusChanged')?.resourceId).toBe(caseId);
    expect(eventByType.get('CaseResolved')?.resourceId).toBe(caseId);
    expect(eventByType.get('TaskCreated')?.metadata.caseId).toBe(caseId);
    expect(eventByType.get('DocumentUploaded')?.metadata.caseId).toBe(caseId);
    expect(eventByType.get('ApprovalRequested')?.metadata.caseId).toBe(caseId);
  });

  it('generates multiple tenants, actors, and related resource groups for larger batches', () => {
    const records = createDummyEvents(100);

    expect(new Set(records.map(({ event }) => event.tenantId)).size).toBe(10);
    expect(
      new Set(records.map(({ event }) => event.actorId)).size,
    ).toBeGreaterThan(1);
    expect(
      new Set(records.map(({ event }) => event.resourceId)).size,
    ).toBeGreaterThan(10);
  });

  it('generates a configurable selected event type', () => {
    const records = createDummyEvents(7, 'DocumentScanned');

    expect(records).toHaveLength(7);
    records.forEach(({ topic, event }) => {
      expect(event.eventType).toBe('DocumentScanned');
      expect(topic).toBe('resolve.document.scanned');
      expect(() => validateAuditEvent(topic, event)).not.toThrow();
    });
  });

  it('reuses one identical event ID and payload for duplicate delivery tests', () => {
    const records = createDuplicateEvents('CaseCreated', 3);

    expect(new Set(records.map(({ event }) => event.eventId)).size).toBe(1);
    expect(new Set(records.map(({ value }) => value)).size).toBe(1);
  });

  it.each([
    ['invalid-json', 'INVALID_JSON'],
    ['invalid-envelope', 'INVALID_ENVELOPE'],
    ['unsupported-version', 'UNSUPPORTED_EVENT_VERSION'],
  ] as const)(
    'creates the requested invalid scenario: %s',
    (scenario, reason) => {
      const { topic, value } = createInvalidEvent(scenario, 'CaseCreated');

      expect(() =>
        validateAuditEvent(topic, parseJsonEvent(Buffer.from(value))),
      ).toThrow(expect.objectContaining({ reason }));
    },
  );
});
