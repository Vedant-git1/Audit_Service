ALTER TABLE "audit_events" RENAME TO "audit_logs";
ALTER TABLE "audit_logs" RENAME CONSTRAINT "audit_events_pkey" TO "audit_logs_pkey";
ALTER TABLE "audit_logs" RENAME COLUMN "organization_id" TO "tenant_id";
ALTER TABLE "audit_logs" RENAME COLUMN "action" TO "event_type";
ALTER TABLE "audit_logs" RENAME COLUMN "created_at" TO "event_timestamp";

ALTER TABLE "audit_logs"
    ADD COLUMN "event_id" UUID,
    ADD COLUMN "event_version" INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN "topic" VARCHAR(255) NOT NULL DEFAULT 'legacy',
    ADD COLUMN "partition" INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN "offset" BIGINT,
    ADD COLUMN "payload" JSONB,
    ADD COLUMN "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP;

WITH numbered_logs AS (
    SELECT "id", ROW_NUMBER() OVER (ORDER BY "id") - 1 AS "legacy_offset"
    FROM "audit_logs"
)
UPDATE "audit_logs" AS logs
SET "event_id" = gen_random_uuid(),
    "offset" = numbered_logs."legacy_offset",
    "payload" = jsonb_build_object(
        'legacy', TRUE,
        'oldValue', logs."old_value",
        'newValue', logs."new_value",
        'requestId', logs."request_id",
        'traceId', logs."trace_id"
    )
FROM numbered_logs
WHERE logs."id" = numbered_logs."id";

ALTER TABLE "audit_logs"
    ALTER COLUMN "event_id" SET NOT NULL,
    ALTER COLUMN "payload" SET NOT NULL,
    ALTER COLUMN "event_version" DROP DEFAULT,
    ALTER COLUMN "topic" DROP DEFAULT,
    ALTER COLUMN "partition" DROP DEFAULT,
    ALTER COLUMN "offset" SET NOT NULL;

ALTER TABLE "audit_logs"
    ADD CONSTRAINT "audit_logs_event_id_key" UNIQUE ("event_id"),
    ADD CONSTRAINT "audit_logs_event_version_positive" CHECK ("event_version" > 0),
    ADD CONSTRAINT "audit_logs_partition_nonnegative" CHECK ("partition" >= 0),
    ADD CONSTRAINT "audit_logs_offset_nonnegative" CHECK ("offset" >= 0);

CREATE INDEX "audit_logs_tenant_event_timestamp_idx"
    ON "audit_logs"("tenant_id", "event_timestamp");
CREATE INDEX "audit_logs_resource_event_timestamp_idx"
    ON "audit_logs"("resource_type", "resource_id", "event_timestamp");
CREATE INDEX "audit_logs_actor_event_timestamp_idx"
    ON "audit_logs"("actor_id", "event_timestamp");
CREATE INDEX "audit_logs_event_timestamp_idx"
    ON "audit_logs"("event_timestamp");
CREATE INDEX "audit_logs_kafka_coordinates_idx"
    ON "audit_logs"("topic", "partition", "offset");

CREATE TABLE "audit_event_processing" (
    "id" UUID NOT NULL DEFAULT gen_random_uuid(),
    "event_id" UUID,
    "topic" VARCHAR(255) NOT NULL,
    "partition" INTEGER NOT NULL,
    "offset" BIGINT NOT NULL,
    "processing_status" VARCHAR(20) NOT NULL,
    "attempt_count" INTEGER NOT NULL DEFAULT 1,
    "error_message" TEXT,
    "processed_at" TIMESTAMPTZ(6),
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "audit_event_processing_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "audit_event_processing_kafka_coordinates_key" UNIQUE ("topic", "partition", "offset"),
    CONSTRAINT "audit_event_processing_partition_nonnegative" CHECK ("partition" >= 0),
    CONSTRAINT "audit_event_processing_offset_nonnegative" CHECK ("offset" >= 0),
    CONSTRAINT "audit_event_processing_attempt_count_positive" CHECK ("attempt_count" > 0),
    CONSTRAINT "audit_event_processing_status_valid"
        CHECK ("processing_status" IN ('PROCESSED', 'DUPLICATE', 'INVALID'))
);

CREATE INDEX "audit_event_processing_event_id_idx"
    ON "audit_event_processing"("event_id");
CREATE INDEX "audit_event_processing_status_created_at_idx"
    ON "audit_event_processing"("processing_status", "created_at");
