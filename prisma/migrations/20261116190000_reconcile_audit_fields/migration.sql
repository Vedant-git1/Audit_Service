ALTER TABLE "audit_logs"
    RENAME COLUMN "event_type" TO "action";

ALTER TABLE "audit_logs"
    RENAME COLUMN "event_timestamp" TO "timestamp";

ALTER TABLE "audit_event_processing"
    ADD COLUMN "raw_message" BYTEA,
    ADD COLUMN "kafka_headers" JSONB;

INSERT INTO "audit_event_processing" (
    "event_id",
    "topic",
    "partition",
    "offset",
    "processing_status",
    "attempt_count",
    "error_message",
    "processed_at",
    "created_at",
    "updated_at"
)
SELECT
    "event_id",
    "topic",
    "partition",
    "offset",
    'PROCESSED',
    1,
    NULL,
    "created_at",
    "created_at",
    "created_at"
FROM "audit_logs"
ON CONFLICT ("topic", "partition", "offset") DO NOTHING;

DROP INDEX "audit_logs_kafka_coordinates_idx";

ALTER TABLE "audit_logs"
    DROP COLUMN "topic",
    DROP COLUMN "partition",
    DROP COLUMN "offset";
