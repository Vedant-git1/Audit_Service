# Resolve Audit Service

The Audit Service is an independently deployable Resolve microservice. It owns
its PostgreSQL database and is designed to consume the domain events documented
by the Resolve Kafka contract. It provides one tenant-scoped audit retrieval
endpoint at `GET /api/v1/audit`, protected by Resolve JWT authentication.
Because no Audit-view permission or RBAC integration contract is currently
defined, the runtime authorization adapter denies reads by default.

## Prerequisites

- Node.js 20 or newer and npm
- A running PostgreSQL database owned by this service
- Access to Kafka with the ten Resolve topics provisioned when testing ingestion
- Resolve Authentication JWKS and Redis revocation-state access for authenticated
  API requests; an approved RBAC adapter is additionally required to enable
  audit reads

## Installation

```powershell
Copy-Item .env.example .env
npm ci
npm run prisma:generate
```

Set the values in `.env` before starting the service. The example values are
development placeholders, not production credentials. Configure `REDIS_URL`,
`JWT_ISSUER`, and `JWT_JWKS_URL` for authentication. Redis must be reachable
for readiness and revocation checks. Use existing development instances of
PostgreSQL, Kafka, Redis and Authentication; this guide does not start
containers.

## Environment configuration

See [.env.example](.env.example). Configuration is validated with Zod at
startup. Kafka is disabled by default for local development because no shared
Resolve broker deployment is included in this repository. Production startup
requires Kafka to be enabled.

## Run locally

Configure `DATABASE_URL` for a running PostgreSQL instance, generate Prisma
Client, apply the checked-in migrations, and run the development server:

```powershell
npm run prisma:generate
npm run prisma:deploy
npm run dev
```

`prisma:deploy` applies pending migrations without resetting the database.
Use `prisma:migrate` only when intentionally authoring a schema migration.

The service listens on `http://localhost:3000`. PostgreSQL and Redis must be
available. To enable API authentication, configure a reachable Authentication
JWKS endpoint and Redis instance containing Resolve session revocation keys.
Until an approved audit-read authorization adapter is connected, API requests
are denied even when their JWT is valid. If the shared Kafka cluster is
available, set `KAFKA_ENABLED=true` and configure its bootstrap servers and
documented topic subscriptions in `.env`.

Container configuration remains in the repository but is not required for the
independent local workflow described here.

## Checks

```powershell
npm run validate:env
npm run typecheck
npm run lint
npm test
npm run test:integration
npm run build
```

The integration test requires PostgreSQL to be running with migrations applied
and `RUN_DB_TESTS=true`. It is skipped otherwise.

## Dummy Kafka event producer

The TypeScript utility publishes synthetic events to all ten actual Resolve
topics. It generates valid UUIDs, ISO timestamps, related case/task/document/
approval resources, multiple tenants and actors, and request/trace headers.
Normal valid-event publishing is separate from deliberately malformed
quarantine scenarios.

Set `KAFKA_ENABLED=true` and `KAFKA_BOOTSTRAP_SERVERS` in `.env`, ensure the
topics already exist, and run:

```powershell
npm run events:one-each
npm run events:10
npm run events:100
npm run events:type -- CaseCreated --count 10
npm run events:duplicates -- --type CaseCreated --count 2
```

`events:type` supports `CaseCreated`, `CaseAssigned`, `CaseStatusChanged`,
`CaseResolved`, `TaskCreated`, `TaskCompleted`, `DocumentUploaded`,
`DocumentScanned`, `ApprovalRequested`, and `ApprovalCompleted`. The duplicate
command republishes one identical event ID and payload at distinct Kafka
offsets.

For intentional quarantine testing only:

```powershell
npm run events:invalid-json -- --count 1
npm run events:invalid-envelope -- --count 1
npm run events:unsupported-version -- --count 1
```

These scenarios publish to the corresponding event topic. Run them only in an
isolated test environment; they do not publish to a DLQ or create topics.

Health endpoints:

- `GET /health` — process liveness.
- `GET /health/ready` — PostgreSQL, Redis/authentication and Kafka readiness
  when enabled; Kafka reports `disabled` in local Kafka-off mode.

Audit retrieval:

- `GET /api/v1/audit` — paginated audit records, filtered by `id`,
  `resourceId`, `actorId`, `eventType`, `resourceType`, `from`, and `to`.
  Pagination uses `page` (zero-based) and `size` (1–100); sorting uses
  `sort=<field>,<direction>`; fields are `timestamp`, `createdAt`, `action`,
  `resourceType`, and `actorId`, with `asc` or `desc` direction.
  The response contains `content`, `page`, `size`, `totalElements`, and
  `totalPages`.
- The request must include `Authorization: Bearer <Resolve access token>`.
  JWT signature, issuer, expiration, access-token type, identity and tenant
  claims are validated; token revocation is checked through Redis.
- Every repository query is scoped to the tenant from the verified JWT. A
  client-supplied tenant ID is not accepted.
- The runtime RBAC adapter currently fails closed and returns `403` until
  Resolve defines the Audit-view permission and integration contract.

## Project structure

```text
src/
  config/                 Environment validation
  infrastructure/
    database/              Prisma and PostgreSQL connection
    kafka/                 KafkaJS consumer connection and subscriptions
    redis/                 Authentication token-revocation lookup
  middleware/              Request IDs, HTTP logging, authentication and authorization
  modules/
    audit/                 Event ingestion, persistence and retrieval
    health/                Liveness and readiness routes
  shared/                  Logger and centralized HTTP errors
  app.ts                   Express application assembly
  server.ts                Runtime startup and graceful shutdown
prisma/                    PostgreSQL schema and migrations
scripts/dummy-events/      Standalone synthetic Kafka producer
tests/                     Jest and Supertest tests
docs/                      Audit Service technical documentation
integration.md             Integration guide for Resolve services
```

For service boundaries, canonical event dependencies, and outstanding contract
decisions, see [integration.md](integration.md) and
[docs/Audit_Service.md](docs/Audit_Service.md).
