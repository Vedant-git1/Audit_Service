import 'dotenv/config';
import { randomUUID } from 'node:crypto';
import { Kafka, logLevel, type ProducerRecord } from 'kafkajs';
import {
  createDuplicateEvents,
  createDummyEvents,
  createInvalidEvent,
  dummyEventTypes,
  type DummyKafkaRecord,
} from './event-factory.js';
import type { AuditEventType } from '../../src/modules/audit/audit-event.schema.js';

type Scenario =
  | 'all'
  | 'batch'
  | 'type'
  | 'duplicates'
  | 'invalid-json'
  | 'invalid-envelope'
  | 'unsupported-version';

function parseCount(value: string | undefined, defaultValue: number): number {
  if (value === undefined) return defaultValue;
  const count = Number(value);
  if (!Number.isInteger(count) || count < 1 || count > 100_000) {
    throw new Error('--count must be an integer between 1 and 100000');
  }
  return count;
}

function parseArguments(args: string[]): {
  scenario: Scenario;
  count: number;
  eventType?: AuditEventType;
} {
  const [scenarioValue, ...options] = args;
  const validScenarios: Scenario[] = [
    'all',
    'batch',
    'type',
    'duplicates',
    'invalid-json',
    'invalid-envelope',
    'unsupported-version',
  ];
  const scenario = (scenarioValue ?? 'all') as Scenario;
  if (!validScenarios.includes(scenario)) {
    throw new Error(
      `Unknown scenario "${scenarioValue}". Use: ${validScenarios.join(', ')}`,
    );
  }

  let countValue: string | undefined;
  let eventTypeValue: string | undefined;
  for (let index = 0; index < options.length; index += 1) {
    const option = options[index];
    if (option === '--count') {
      countValue = options[index + 1];
      index += 1;
    } else if (option === '--type') {
      eventTypeValue = options[index + 1];
      index += 1;
    } else if (!option?.startsWith('--') && eventTypeValue === undefined) {
      eventTypeValue = option;
    } else {
      throw new Error(`Unknown or incomplete option "${option}"`);
    }
  }

  const count = parseCount(countValue, scenario === 'all' ? 10 : 1);
  const eventType = eventTypeValue as AuditEventType | undefined;
  if (eventType && !dummyEventTypes.includes(eventType)) {
    throw new Error(
      `Unknown event type "${eventType}". Supported types: ${dummyEventTypes.join(', ')}`,
    );
  }
  if (scenario === 'type' && !eventType) {
    throw new Error('The type scenario requires an event type');
  }
  if (scenario === 'duplicates' && count < 2) {
    throw new Error('The duplicates scenario requires --count of at least 2');
  }
  if (scenario === 'all' && eventType) {
    throw new Error('The all scenario does not accept an event type');
  }
  if (scenario === 'all' && count !== dummyEventTypes.length) {
    throw new Error('The all scenario always publishes one event per type');
  }
  if (scenario.startsWith('invalid-') || scenario === 'unsupported-version') {
    if (count > 1_000) {
      throw new Error('Invalid-message scenarios are limited to 1000 records');
    }
  }

  return {
    scenario,
    count,
    ...(eventType ? { eventType } : {}),
  };
}

function createRecords(
  scenario: Scenario,
  count: number,
  eventType?: AuditEventType,
): DummyKafkaRecord[] {
  switch (scenario) {
    case 'all':
    case 'batch':
      return createDummyEvents(count);
    case 'type':
      if (!eventType)
        throw new Error('The type scenario requires an event type');
      return createDummyEvents(count, eventType);
    case 'duplicates':
      return createDuplicateEvents(eventType ?? 'CaseCreated', count);
    case 'invalid-json':
    case 'invalid-envelope':
    case 'unsupported-version':
      return Array.from({ length: count }, () =>
        createInvalidEvent(scenario, eventType),
      );
  }
}

function toKafkaRecord(record: DummyKafkaRecord): ProducerRecord {
  return {
    topic: record.topic,
    messages: [
      {
        key: record.key,
        value: record.value,
        headers: {
          'x-request-id': randomUUID(),
          'x-trace-id': randomUUID(),
        },
      },
    ],
  };
}

async function main(): Promise<void> {
  const { scenario, count, eventType } = parseArguments(process.argv.slice(2));
  const bootstrapServers = (process.env.KAFKA_BOOTSTRAP_SERVERS ?? '')
    .split(',')
    .map((server) => server.trim())
    .filter(Boolean);
  if (bootstrapServers.length === 0) {
    throw new Error('Set KAFKA_BOOTSTRAP_SERVERS before publishing events');
  }

  const records = createRecords(scenario, count, eventType);
  const kafka = new Kafka({
    clientId: `${process.env.KAFKA_CLIENT_ID ?? 'audit-service'}-dummy-events`,
    brokers: bootstrapServers,
    logLevel: logLevel.WARN,
  });
  const producer = kafka.producer({ allowAutoTopicCreation: false });

  try {
    await producer.connect();
    for (const record of records) {
      await producer.send(toKafkaRecord(record));
    }
    const topicCounts = records.reduce<Record<string, number>>(
      (totals, record) => {
        totals[record.topic] = (totals[record.topic] ?? 0) + 1;
        return totals;
      },
      {},
    );
    process.stdout.write(
      `${JSON.stringify(
        {
          scenario,
          count: records.length,
          eventType: eventType ?? null,
          topics: topicCounts,
          duplicateEventId:
            scenario === 'duplicates' ? records[0]?.event.eventId : undefined,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await producer.disconnect();
  }
}

void main().catch((error: unknown) => {
  process.stderr.write(
    `${error instanceof Error ? error.message : 'Dummy event publishing failed'}\n`,
  );
  process.exitCode = 1;
});
