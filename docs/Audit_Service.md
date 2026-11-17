# Audit Service Technical Overview

## Scope and status

The service consumes Resolve's documented domain events and writes immutable
audit rows. It exposes a tenant-scoped retrieval endpoint at
`GET /api/v1/audit`. JWT authentication and Redis-backed revocation checks are
implemented. Audit-read authorization intentionally fails closed until Resolve
provides an approved RBAC permission and integration contract.

## Runtime architecture

```text
KafkaJS consumer group → Zod event validation → ingestion service
  → repository transaction → audit_logs + audit_event_processing
  → explicit Kafka offset commit
GET /api/v1/audit → JWT authentication → fail-closed RBAC adapter
  → query validation → query service → tenant-scoped Prisma repository
HTTP health routes → dependency readiness
```

The Express application assembles request-ID middleware, Pino HTTP logging,
health routes, and centralized errors. `/health` is a liveness check.
`/health/ready` checks PostgreSQL, Redis-backed authentication state and, when
enabled, the Kafka connection. Kafka-disabled local mode is explicitly shown
in the readiness response and is rejected in production configuration.

## Audit retrieval API

The canonical route is `GET /api/v1/audit`, as specified in the SRS and Task 9.
No additional record, resource, or actor routes are introduced. Filters are
query parameters:

| Parameter      | Meaning                                                                                                                  |
| -------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `id`           | Audit record UUID                                                                                                        |
| `resourceId`   | Resource UUID                                                                                                            |
| `actorId`      | Actor UUID                                                                                                               |
| `eventType`    | Original PascalCase event type                                                                                           |
| `resourceType` | `CASE`, `TASK`, `DOCUMENT`, or `APPROVAL`                                                                                |
| `from`, `to`   | Inclusive ISO-8601 timestamp bounds                                                                                      |
| `page`, `size` | Zero-based page and page size (1–100)                                                                                    |
| `sort`         | `<field>,<direction>`; fields: `timestamp`, `createdAt`, `action`, `resourceType`, `actorId`; direction: `asc` or `desc` |

The response follows the Organization Service page convention:
`content`, `page`, `size`, `totalElements`, and `totalPages`. The API returns
canonical audit fields and omits the original JSONB payload.

Every repository query receives the tenant ID from the verified JWT and
combines it with all client filters; there is no client-controlled tenant
filter. An `id` filter with no matching record in that tenant returns
`AUDIT_NOT_FOUND`, preventing cross-tenant existence disclosure. Page size and
sort fields/directions are bounded and allowlisted.

Requests require `Authorization: Bearer <token>`. The verifier uses RS256 and
the configured Authentication JWKS endpoint, validates issuer, expiration,
access-token type, and UUID-shaped `sub`, `tenantId`, and `tokenId` claims, then
checks `session:revoked:{tokenId}` in Redis. Redis or JWKS dependency failures
return service unavailable rather than accepting or misclassifying the token.
No role or permission claim is assumed. Since no approved Audit-view permission
or RBAC adapter contract is available, `auditAuthorizationUnavailable` denies
all retrieval requests with `403 AUDIT_FORBIDDEN`; reads remain disabled until
that external dependency is resolved.

There are no update or delete API operations for audit history.

## Kafka event contract

The consumer group is `resolve-audit-service`. It subscribes to exactly these
contract topics:

| Topic                        | Event type          |
| ---------------------------- | ------------------- |
| `resolve.case.created`       | `CaseCreated`       |
| `resolve.case.assigned`      | `CaseAssigned`      |
| `resolve.case.statuschanged` | `CaseStatusChanged` |
| `resolve.case.resolved`      | `CaseResolved`      |
| `resolve.task.created`       | `TaskCreated`       |
| `resolve.task.completed`     | `TaskCompleted`     |
| `resolve.document.uploaded`  | `DocumentUploaded`  |
| `resolve.document.scanned`   | `DocumentScanned`   |
| `resolve.approval.requested` | `ApprovalRequested` |
| `resolve.approval.completed` | `ApprovalCompleted` |

`src/modules/audit/audit-event.schema.ts` validates the shared envelope
(`eventId`, `eventType`, `eventVersion`, `tenantId`, optional/nullable
`actorId`, `resourceType`, `resourceId`, `timestamp`, and `metadata`) and each
event's metadata against
`Resolve_Documentation/Contracts/kafka.md`. UUIDs, timestamp format, resource
type/topic/event type correspondence, and metadata-specific fields are checked.
Only contract version 1 is currently implemented. Invalid messages are recorded
as `INVALID` processing outcomes, including the original bytes and available
Kafka headers, before their offsets are committed.

## Audit field mapping

The canonical audit schema calls the action column `action`; the Kafka contract
uses the PascalCase `eventType`. The database stores that original value in
`audit_logs.action` (for example, `CaseStatusChanged`) rather than maintaining
two duplicate columns. `eventVersion` and the complete original JSON event are
retained separately. The canonical `timestamp` column contains the event
timestamp, while `createdAt` records when the audit row was ingested.

`oldValue` and `newValue` are populated only for documented before/after fields:

| Event               | `old_value`                               | `new_value`            |
| ------------------- | ----------------------------------------- | ---------------------- |
| `CaseStatusChanged` | `metadata.previousStatus`, when present   | `metadata.newStatus`   |
| `CaseAssigned`      | `metadata.previousAssignee`, when present | `metadata.newAssignee` |
| Other events        | `NULL`                                    | `NULL`                 |

No values are inferred for events that do not provide before/after data.

The Kafka contract does not currently define correlation header names.
Ingestion best-effort reads `x-request-id` and `x-trace-id`; it also accepts the
W3C `traceparent` trace ID and `requestId`/`traceId` envelope extensions when
present. Missing or unusable correlation values remain `NULL`. Producer-side
header naming and propagation still require contract agreement.

## Database ownership

`audit_logs` stores the UUID event and tenant/actor/resource identifiers,
original PascalCase action, event version/time, and full JSONB payload.
`event_id` is unique for event-level idempotency. `old_value`, `new_value`,
`request_id`, and `trace_id` are nullable canonical audit fields. Indexes
support tenant, resource, actor, and event-time access. Kafka coordinates are
operational state and live only in `audit_event_processing`. The latest
migration copies existing audit-row coordinates into that tracker before
removing the duplicated columns, so existing audit records and their
coordinates are preserved.

`audit_event_processing` records each Kafka message coordinate, optional event
ID, status, attempt count, error, and processing timestamps. The unique
`(topic, partition, offset)` constraint makes the tracker idempotent per Kafka
message. For invalid messages it also stores raw bytes in `raw_message` (not
JSONB, so malformed JSON remains preservable) and Kafka headers in `kafka_headers`
as JSONB; binary header values are base64 encoded. Existing error text and
original invalid bytes are retained when a coordinate is retried. Processing
statuses are:

| Status      | Meaning                                                                                                                               |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `PROCESSED` | A valid new event was stored in `audit_logs`.                                                                                         |
| `DUPLICATE` | The `event_id` already exists; no second audit row was inserted.                                                                      |
| `INVALID`   | The event was malformed, unsupported, or failed contract validation; raw bytes, headers, and failure reason are stored before commit. |

UUID references to organizations and users do not have foreign keys because
their authoritative data belongs to separate service databases. No other
service database is accessed.

## Initial replay policy

`KAFKA_FROM_BEGINNING` controls KafkaJS `fromBeginning` and defaults to `true`.
It affects a topic partition only when `resolve-audit-service` has no valid
committed offset for that partition; it does not reset or override existing
committed offsets during normal restarts. On first startup, a new group
therefore reads from the earliest offset Kafka still retains. This cannot
recover data already expired by broker retention or otherwise unavailable.

Keep the documented group ID `resolve-audit-service` stable. Changing it
creates a new group with no committed offsets, which can replay all retained
events when `KAFKA_FROM_BEGINNING=true` (or start at the latest when false).
Replay is idempotent by event ID and Kafka message coordinates, but it can
increase database and broker load. Production offsets are never reset by
application startup; group-ID changes and administrative offset resets require
an explicit operational decision. Confirm topic retention and the required
replay window with Kafka operators before first deployment.

## Processing and offsets

`src/infrastructure/kafka/client.ts` disables KafkaJS automatic commits and
allows concurrency across partitions, while serializing work and commits
within each topic-partition. A message is validated and persisted first. The
repository writes a valid audit row and its `PROCESSED` tracker in one Prisma
transaction; an existing event ID becomes `DUPLICATE` without another audit
row. A replay at the same coordinates updates the tracker without downgrading
a successful status. Invalid input is terminally quarantined in
`audit_event_processing` with raw bytes, headers, coordinates, and reason.

Only after that durable outcome completes does the consumer heartbeat and
explicitly commit `message.offset + 1`. Since callbacks for a partition are
serialized, a later offset cannot be committed while an earlier message in
that partition has not completed durable handling. Other partitions continue
independently. Persistence, heartbeat, or offset-commit failure fails the
callback as retriable; no offset is committed for that failed message. A
redelivery is safe through the audit event ID and message-coordinate
constraints.

## Consumer retry, quarantine, and DLQ policy

The Kafka documentation's five-attempt exponential backoff and `{topic}.dlq`
flow is the producer transactional-outbox policy. It is not applied as an
implicit consumer attempt cap. The contract does not define a consumer retry
limit or provisioned consumer DLQ topics.

Transient database errors, Kafka disconnections, heartbeats, and offset-commit
failures remain unacknowledged. KafkaJS retries/restarts the consumer using
its configured retriable-failure behavior; the same committed group position
is resumed, so an unavailable database can pause progress and block later
messages in that partition without silently losing them. The consumer does
not impose a maximum delivery attempt count.

Malformed JSON, unsupported event types/versions, and schema-invalid records
are durably quarantined in `audit_event_processing` as `INVALID`; the database
row is the terminal handling action, and only then is the offset committed.
The service does not publish invalid or repeatedly failing consumer messages
to `{topic}.dlq` and does not create Kafka topics. If operations require an
external consumer DLQ, Resolve must decide its topic provisioning, ACLs,
payload/metadata format, producer permissions, retention, and replay process
before enabling that strategy. Until then, operators must monitor and preserve
the database quarantine records.

## Recovery, readiness, and shutdown

Kafka startup failures are logged and surfaced as not-ready; a later readiness
check retries connection and subscription. Broker/consumer crashes mark Kafka
unready until the consumer rejoins its group. KafkaJS reconnect/restart keeps
the same group ID and committed offsets. Prisma operations are retried by
redelivery after the database becomes available; readiness independently
checks PostgreSQL. There is no automatic production offset reset.

SIGINT/SIGTERM stop new Kafka consumption, wait for in-flight handlers through
KafkaJS stop, then disconnect Kafka and database clients. An in-flight durable
write whose offset was not committed may be redelivered after restart and is
idempotent. Kafka auto-topic creation is disabled.

## Configuration and validation

Required authentication settings are `REDIS_URL` and `JWT_JWKS_URL`;
`JWT_ISSUER` defaults to `resolve-authentication-service`, and
`JWT_AUDIENCE` is optional when Authentication uses an audience claim.
Required Kafka settings when enabled: `KAFKA_ENABLED`,
`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_CLIENT_ID`,
`KAFKA_CONSUMER_GROUP_ID=resolve-audit-service`, and the ten canonical
`KAFKA_TOPICS`. `KAFKA_FROM_BEGINNING` defaults to `true` and may be explicitly
set to `false` for a new group that should start at the latest broker offset.
`DATABASE_URL`, `NODE_ENV`, `PORT`, and `LOG_LEVEL` are also validated at
startup. See `.env.example`.

Run unit tests with `npm test`. To verify actual rows, duplicate idempotency,
JSONB payload persistence, and processing tracker updates against PostgreSQL,
apply migrations and run `RUN_DB_TESTS=true npm run test:integration`.

## Independent Kafka testing

`scripts/dummy-events/producer.ts` uses KafkaJS and the topic/event mappings
from the service contract. It generates valid v1 envelopes with related case,
task, document, and approval IDs; multiple synthetic tenant and actor IDs;
UUID resource/event IDs; ISO timestamps; and correlation headers. Normal event
commands only emit valid records. Separate modes deliberately emit malformed
JSON, a missing envelope field, or an unsupported version for quarantine
verification. The producer uses `resourceId` as the Kafka message key and does
not create topics.

With an existing broker and provisioned topics, the package commands are:

```powershell
npm run events:one-each
npm run events:10
npm run events:100
npm run events:type -- CaseCreated --count 10
npm run events:duplicates -- --type CaseCreated --count 2
npm run events:invalid-json -- --count 1
npm run events:invalid-envelope -- --count 1
npm run events:unsupported-version -- --count 1
```

Duplicate mode repeats the same event ID and payload, while each Kafka produce
gets a separate broker offset. Invalid modes are for isolated test topics or
environments and must not be used against production data.

Unit tests validate all generated normal events against the same local Zod
schemas as the consumer. PostgreSQL integration tests exercise audit and
processing persistence, JSONB payload storage, event-level deduplication,
invalid raw-message preservation, and transaction rollback. Running the dummy
producer while the service consumes is the supported manual broker-to-database
test path.

## End-to-end verification and current limits

The full target path requires an already-running Kafka broker with all ten
topics, the Audit consumer, a migrated PostgreSQL database, and a working
authorized API identity. API reads currently fail closed because the
Audit-view permission/RBAC adapter contract has not been supplied. Therefore,
database and consumer verification can be performed independently, but the
complete Kafka-to-REST acceptance flow cannot pass until that authorization
dependency is integrated. Do not treat unit mocks as broker-level validation.

Before production integration, verify all ten topic subscriptions, event and
payload preservation, tenant/actor/resource IDs, old/new field mapping,
correlation headers, persisted processing statuses, duplicate delivery,
database-down replay, consumer restarts, invalid-message quarantine, and
tenant-filtered API results with the approved authorization adapter.

## Deployment

Deploy the service with its own PostgreSQL database and migrations, broker
connectivity and topic ACLs, Authentication JWKS access, and Redis revocation
access. Keep the documented consumer group stable and make the Kafka retention
window sufficient for recovery. Configure production secrets through the
deployment secret manager. Container definitions remain available but have
not been exercised as part of this application validation.

## Schema and integration gaps

- The canonical Audit API route is implemented, but the Audit-view permission
  and RBAC integration contract remain undefined; runtime authorization denies
  retrieval until Resolve supplies them.
- Redis and Authentication JWKS are external dependencies. The repository's
  local Compose configuration does not provide either dependency.
- The shared Kafka broker is an external Task 1 dependency. Local development
  can run with Kafka disabled; production cannot.
