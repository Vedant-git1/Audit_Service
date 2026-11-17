# Audit Service Integration Guide

## Purpose

The Audit Service is the Resolve-owned service for retaining an append-only
history of significant domain actions. It consumes the documented domain
events from Kafka and records them in its own PostgreSQL database. It exposes
tenant-scoped audit retrieval at `GET /api/v1/audit`.

## Architectural boundaries

- The service owns its PostgreSQL database and the `audit_logs` and
  `audit_event_processing` tables.
- Other Resolve services must not connect directly to this database.
- The service consumes events; it does not produce domain events or accept
  writes or mutations of historical audit records over HTTP.
- `GET /api/v1/audit` is the canonical retrieval endpoint; health checks are
  `GET /health` and `GET /health/ready`.
- Retrieval requires Resolve JWT authentication and tenant context. The runtime
  currently denies all reads until the Audit-view RBAC permission and adapter
  contract are approved.
- Business services remain independent of Audit availability; the audit trail
  is eventually consistent through the documented Kafka event flow.

## Technology stack

Node.js, TypeScript, Express, Prisma, PostgreSQL, KafkaJS, Zod, Pino, Jest,
Supertest, Docker, and Docker Compose.

## Kafka dependency and subscriptions

The canonical topic names, event envelope, payload schemas, consumer-group
guidance, and delivery semantics are owned by
`Resolve_Documentation/Contracts/kafka.md`. The Audit Service configuration
uses the contract's `resolve-audit-service` consumer group and subscribes to
all ten documented topics:

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

Do not add or rename topics in this repository.

Kafka can be disabled for local foundation work because this workspace does not
contain a shared Kafka deployment. Production configuration rejects a disabled
Kafka connection.

### Consumer group and initial replay

The group ID is `resolve-audit-service`; keep it stable across normal restarts.
`KAFKA_FROM_BEGINNING` defaults to `true` and applies only when a partition has
no valid committed offset for that group. A new group therefore starts at the
earliest offset still retained by Kafka. It does not restore expired data.
Setting it to `false` for a group without offsets starts at the latest available
offset and can skip retained history.

Changing the consumer group ID creates a new group. With the default setting it
may replay all retained events and create additional processing load; event-ID
idempotency prevents duplicate audit records. Existing committed offsets are
preserved on ordinary restarts, and the service never resets production offsets
automatically. Confirm topic retention and replay requirements with Kafka
operators before initial deployment or any group-ID change.

## Event envelope and validation

Each message must contain the contract envelope fields `eventId`, `eventType`,
`eventVersion`, `tenantId`, `actorId` (nullable or absent where permitted),
`resourceType`, `resourceId`, `timestamp`, and event-specific `metadata`.
Zod validates UUIDs, timestamps, event/topic/resource correspondence, and the
metadata schema for each topic. Version 1 is currently supported. The original
complete event object is stored in JSONB; it is not reconstructed from the
validated metadata.

Common envelope example:

```json
{
  "eventId": "b2e1a4c0-1111-4a2b-9c3d-000000000010",
  "eventType": "CaseCreated",
  "eventVersion": 1,
  "tenantId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "actorId": "9e1071ea-9a3b-4a2f-9e2e-1a2b3c4d5e6f",
  "resourceType": "CASE",
  "resourceId": "c1a2b3d4-0000-4000-8000-000000000042",
  "timestamp": "2026-09-08T10:00:00Z",
  "metadata": {
    "caseId": "c1a2b3d4-0000-4000-8000-000000000042"
  }
}
```

`actorId` may be `null` or absent. IDs use UUID format; timestamps use
ISO-8601 date-time format; `eventVersion` must currently equal `1`.
`resourceType` must match the event type, and `resourceId` is the Kafka
partition key (`key`).

| Topic                        | Event type          | `resourceType` | Required metadata                                                                                                        | Optional metadata                                                  |
| ---------------------------- | ------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------ |
| `resolve.case.created`       | `CaseCreated`       | `CASE`         | `caseId: UUID`                                                                                                           | —                                                                  |
| `resolve.case.assigned`      | `CaseAssigned`      | `CASE`         | `caseId: UUID`, `newAssignee: UUID`                                                                                      | `previousAssignee: UUID \| null`                                   |
| `resolve.case.statuschanged` | `CaseStatusChanged` | `CASE`         | `caseId: UUID`, `newStatus: OPEN \| ASSIGNED \| INVESTIGATION \| REVIEW \| APPROVED \| REJECTED \| RESOLVED \| REOPENED` | `previousStatus`: same status enum or `null`                       |
| `resolve.case.resolved`      | `CaseResolved`      | `CASE`         | `caseId: UUID`                                                                                                           | —                                                                  |
| `resolve.task.created`       | `TaskCreated`       | `TASK`         | `taskId: UUID`, `caseId: UUID`                                                                                           | `assignedToUserId: UUID \| null`, `assignedToTeamId: UUID \| null` |
| `resolve.task.completed`     | `TaskCompleted`     | `TASK`         | `taskId: UUID`, `caseId: UUID`, `completedAt: date-time`                                                                 | —                                                                  |
| `resolve.document.uploaded`  | `DocumentUploaded`  | `DOCUMENT`     | `documentId: UUID`, `caseId: UUID`, `storageKey: string`                                                                 | —                                                                  |
| `resolve.document.scanned`   | `DocumentScanned`   | `DOCUMENT`     | `documentId: UUID`, `caseId: UUID`, `scanStatus: CLEAN \| INFECTED \| FAILED`                                            | —                                                                  |
| `resolve.approval.requested` | `ApprovalRequested` | `APPROVAL`     | `approvalId: UUID`, `caseId: UUID`                                                                                       | `approverId: UUID \| null`                                         |
| `resolve.approval.completed` | `ApprovalCompleted` | `APPROVAL`     | `approvalId: UUID`, `caseId: UUID`, `decision: APPROVED \| REJECTED \| EXPIRED`                                          | —                                                                  |

These are the v1 Kafka contracts, not an independent simplified schema.
Producers must publish the full envelope to the topic matching `eventType`.

### Mapping to the audit record

Resolve's canonical audit schema names the operation field `action`; the Kafka
contract names it `eventType`. Audit Service stores the original PascalCase
event type in `audit_logs.action` (for example, `CaseCreated`) and does not
create a second, duplicate `event_type` column. The full event, including its
original `eventType`, remains in the JSONB payload. The audit `timestamp` is
the source event time; `created_at` is the ingestion time.

`CaseStatusChanged` maps `metadata.previousStatus`/`metadata.newStatus` to
`old_value`/`new_value`. `CaseAssigned` maps
`metadata.previousAssignee`/`metadata.newAssignee`. Other events leave those
fields null; the service does not invent before/after values.

The current Resolve Kafka contract does not define correlation header names.
The consumer best-effort maps `x-request-id`, `x-trace-id`, W3C `traceparent`,
or envelope extension fields `requestId`/`traceId`. Missing values remain
nullable. Producers and the platform contract still need to ratify correlation
header names and propagation.

## Audit retrieval API

The only audit retrieval route is `GET /api/v1/audit`. It supports these
query parameters:

| Parameter      | Meaning                                                                                                                |
| -------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `id`           | Audit record UUID                                                                                                      |
| `resourceId`   | Resource UUID                                                                                                          |
| `actorId`      | Actor UUID                                                                                                             |
| `eventType`    | Original PascalCase event type                                                                                         |
| `resourceType` | `CASE`, `TASK`, `DOCUMENT`, or `APPROVAL`                                                                              |
| `from`, `to`   | Inclusive ISO-8601 timestamp bounds                                                                                    |
| `page`, `size` | Zero-based page and page size; size is limited to 1–100                                                                |
| `sort`         | `<field>,<direction>`; fields `timestamp`, `createdAt`, `action`, `resourceType`, `actorId`; direction `asc` or `desc` |

Example:

```http
GET /api/v1/audit?resourceType=CASE&from=2026-01-01T00%3A00%3A00Z&page=0&size=20&sort=timestamp%2Cdesc
Authorization: Bearer <Resolve access token>
```

Successful responses use the Organization Service pagination shape:

```json
{
  "content": [],
  "page": 0,
  "size": 20,
  "totalElements": 0,
  "totalPages": 0
}
```

The API returns audit metadata and canonical fields; the original payload is
not returned. Every database query includes the tenant ID extracted from the
verified JWT. `tenantId` is not accepted as a client query parameter. A
specific `id` that does not exist in the authenticated tenant returns
`AUDIT_NOT_FOUND`. Filters and sort values are validated at the HTTP boundary,
and results are paginated in PostgreSQL.

Authentication uses an RS256 JWT selected by `kid` from `JWT_JWKS_URL`.
`JWT_ISSUER` defaults to `resolve-authentication-service`; `JWT_AUDIENCE` is
optional and should only be set if Authentication issues that audience. The
token must have a valid expiry, `type: access`, and UUID `sub`, `tenantId`, and
`tokenId` claims. Redis checks the Resolve revocation key
`session:revoked:{tokenId}`; inability to check revocation fails closed and
returns an availability error. JWKS endpoint outages likewise return
`503 AUTH_PROVIDER_UNAVAILABLE`; they are not reported as invalid credentials.

No roles or permission names are inferred. There is no approved Audit-view
permission or RBAC adapter contract in the available Resolve documentation.
Until that dependency is provided, the service returns `403 AUDIT_FORBIDDEN`
for every audit read, including otherwise valid authenticated requests.

## Database ownership

PostgreSQL is the system of record. Prisma migrations define two owned tables:

- `audit_logs` stores one immutable row per unique `event_id`, including tenant,
  optional actor, action (mapped from the original PascalCase event type),
  event version, resource, event timestamp, canonical optional old/new values
  and correlation IDs, and the full event JSONB payload.
- `audit_event_processing` stores status and attempt/error timestamps per
  unique `(topic, partition, offset)`, along with the event ID. This is the
  authoritative store for Kafka coordinates and processing state. Invalid
  messages also retain their raw bytes, Kafka headers, and validation reason;
  bytes are stored as `BYTEA` so malformed JSON is preserved. Binary header
  values are represented as Base64 in the JSONB header map. Statuses are
  `PROCESSED`, `DUPLICATE`, and `INVALID`.

`PROCESSED` means a new valid event was stored; `DUPLICATE` means its event ID
already exists and no second audit row was inserted; `INVALID` means a
terminally invalid event was durably recorded, including raw bytes and headers,
before its Kafka offset was committed. Replays at the same coordinates
increment the attempt count and preserve successful processing status and
existing error evidence.

Indexes cover tenant/event time, resource/event time, actor/event time,
event time, processing event ID, and processing status/creation time.

Do not query another service's database to resolve organization or actor
references.

The consolidated schema describes foreign keys from audit records to
Organization and User tables, but those tables belong to separate service
databases. This service therefore stores their UUIDs as reference values and
does not create cross-service database foreign keys.

## Environment configuration

Copy `.env.example` to `.env` and provide endpoints/credentials for already
available dependencies. Configuration is validated at process startup.
Production requires Kafka to be enabled.

| Variable                                        | Purpose                                                                                   |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `DATABASE_URL`                                  | PostgreSQL URL for the Audit-owned database                                               |
| `KAFKA_ENABLED`                                 | Enables the consumer; production must set `true`                                          |
| `KAFKA_BOOTSTRAP_SERVERS`                       | Comma-separated broker host:port list                                                     |
| `KAFKA_CLIENT_ID`                               | Kafka client identity                                                                     |
| `KAFKA_CONSUMER_GROUP_ID`                       | Must remain `resolve-audit-service`                                                       |
| `KAFKA_TOPICS`                                  | Exactly the ten contract topics; checked against the contract                             |
| `KAFKA_FROM_BEGINNING`                          | New-group initial replay setting; defaults `true`; existing group commits take precedence |
| `REDIS_URL`                                     | Resolve session revocation-state lookup and readiness                                     |
| `JWT_JWKS_URL`                                  | Resolve Authentication RS256 JWKS endpoint                                                |
| `JWT_ISSUER`                                    | Expected issuer; defaults `resolve-authentication-service`                                |
| `JWT_AUDIENCE`                                  | Optional expected audience, only when issued by Authentication                            |
| `NODE_ENV`, `PORT`, `SERVICE_NAME`, `LOG_LEVEL` | Runtime environment, HTTP port, service identity, and log filtering                       |

Never put production credentials in `.env.example`, Docker images, or source
control. Use the deployment secret manager for production values.

## Local startup

The service can be run independently against already available PostgreSQL,
Kafka, Redis, and Authentication endpoints; this workflow does not start
containers. First set `DATABASE_URL` and the external dependency settings in
`.env`, then:

```powershell
Copy-Item .env.example .env
npm ci
npm run prisma:generate
npm run prisma:deploy
npm run dev
```

`npm run prisma:deploy` applies the checked-in incremental Prisma migrations.
`npm run prisma:migrate` is for authoring a new migration during development.
`npm run typecheck`, `npm run lint`, `npm test`, and `npm run build` are
lightweight code checks. `RUN_DB_TESTS=true npm run test:integration` requires
the configured database to be available and migrations to have been applied.

## Dummy event producer

The standalone TypeScript/KafkaJS producer is under
`scripts/dummy-events/`. It emits the canonical v1 event envelope and metadata
for every event type, to the actual topics listed above. Records have valid UUID
identifiers, ISO timestamps, realistic links among cases/tasks/documents/
approvals, multiple synthetic tenants/actors across batches, resource ID Kafka
keys, and `x-request-id`/`x-trace-id` headers. The producer expects topics to
exist and uses `allowAutoTopicCreation: false`.

Set `KAFKA_ENABLED=true` and `KAFKA_BOOTSTRAP_SERVERS` in `.env`, ensure all
topics are provisioned, and run:

```powershell
npm run events:one-each
npm run events:10
npm run events:100
npm run events:type -- CaseCreated --count 10
npm run events:duplicates -- --type CaseCreated --count 2
```

Supported selected event types: `CaseCreated`, `CaseAssigned`,
`CaseStatusChanged`, `CaseResolved`, `TaskCreated`, `TaskCompleted`,
`DocumentUploaded`, `DocumentScanned`, `ApprovalRequested`, and
`ApprovalCompleted`. `events:one-each` publishes one per type; the batch
commands rotate through all ten types. Duplicate mode publishes the same
eventId and exact payload repeatedly at distinct offsets.

Deliberately invalid scenarios are separate commands and are intended only for
isolated test environments:

```powershell
npm run events:invalid-json -- --count 1
npm run events:invalid-envelope -- --count 1
npm run events:unsupported-version -- --count 1
```

Malformed payloads are sent to the corresponding event topic (not a DLQ), so
the consumer can durably quarantine them before committing.

## Expected external dependencies

- Resolve's shared Kafka cluster and topic provisioning (Task 1) before
  enabling event consumption.
- Resolve Authentication JWKS availability, Redis revocation-state storage,
  and the Audit-view permission/RBAC adapter before audit reads can be enabled.
- PostgreSQL provisioning or Compose-managed local PostgreSQL.

## Integration considerations and open contract work

- The SRS and Task 9 establish `GET /api/v1/audit`; no extra REST routes have
  been added. The RBAC permission and adapter are still an external blocker,
  and the current adapter intentionally denies reads.
- Event delivery is at-least-once. A database transaction writes the audit row
  and processing status before the consumer explicitly commits the next Kafka
  offset. Work and commits are serialized within each partition, preventing a
  later message from being acknowledged before an earlier message is durable;
  independent partitions can process concurrently. Unique event ID and
  message-coordinate constraints make replay safe.
- Invalid JSON, unsupported event types/versions, and schema failures are
  durably recorded as `INVALID`, with their raw bytes and available Kafka
  headers, topic, partition, offset, and validation reason before offset
  commit. This database record is the consumer's current quarantine mechanism.
  Database/Kafka failures do not commit offsets.
- The Kafka contract's five-attempt `{topic}.dlq` rule applies to producer
  outbox publishing; it is not assumed to be the consumer retry policy. The
  contract does not specify a consumer attempt cap or provision consumer DLQ
  topics. Transient processing/commit failures remain unacknowledged and KafkaJS
  restarts/retries the same consumer group without a configured attempt limit.
  Invalid messages are committed only after durable database quarantine. The
  service neither publishes to a consumer DLQ nor creates Kafka topics.
- If an external consumer DLQ is required, Resolve/Kafka operators must decide
  topic provisioning and ACLs, producer permissions, retry threshold, payload
  and metadata format, retention, monitoring, and replay procedures. Until that
  decision is approved, operators should monitor and preserve `INVALID`
  processing rows; an unavailable database blocks the affected partition
  rather than losing or bypassing the message.
- Kafka readiness is false during a consumer restart and becomes ready after a
  successful group rejoin. SIGINT/SIGTERM stop consumption and wait for
  in-flight handlers before disconnecting. Any durable write not followed by a
  successful offset commit may be redelivered and is idempotent.
- Run integration verification after PostgreSQL is available:
  `RUN_DB_TESTS=true npm run test:integration`.

## Integration checklist

- [ ] Provision the Audit-owned PostgreSQL database and apply migrations.
- [ ] Provision all ten Kafka topics with the Resolve contract's partition and
      retention policy; do not rely on consumer auto-topic creation.
- [ ] Grant the Audit consumer read/group permissions and keep group ID
      `resolve-audit-service` stable.
- [ ] Confirm retained history is sufficient for the required first-start and
      recovery replay window; review any group-ID changes operationally.
- [ ] Set `KAFKA_ENABLED=true`, broker list, and required Kafka environment.
- [ ] Make Authentication JWKS and Redis revocation state reachable.
- [ ] Supply the approved Audit-view RBAC permission/adapter before expecting
      successful API reads; the current runtime intentionally returns 403.
- [ ] Run unit, database integration, and dummy-event validation in isolated
      environments. Do not represent mocked tests as broker E2E results.
- [ ] Verify valid, duplicate, malformed, database-outage, restart, and
      tenant-filtered query scenarios against real dependencies before release.

Health checks:

- `GET /health` reports process liveness and request correlation identifiers.
- `GET /health/ready` checks PostgreSQL, Redis authentication-state access, and
  Kafka connectivity/group recovery when Kafka is enabled. Its response makes
  disabled Kafka explicit; readiness is not a substitute for an end-to-end
  consumption check.

## Known limitations

- Kafka and PostgreSQL are external; the repository does not bundle or start
  either service in this integration workflow.
- No consumer DLQ is provisioned or published to. Invalid records are
  quarantined in `audit_event_processing`; transient failures remain uncommitted.
- The Audit-view RBAC permission and adapter are unresolved, so API reads are
  fail-closed and cannot yet complete authenticated REST E2E verification.
- API response records do not expose the original JSONB payload; that payload
  is retained in the database for audit preservation and direct authorized
  operational inspection.
- Kafka broker-level replay, rebalance, and recovery behavior must be validated
  against the deployed Kafka infrastructure; unit tests use mocks.
