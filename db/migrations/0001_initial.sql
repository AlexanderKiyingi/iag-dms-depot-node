-- Depot-node schema: the offline document buffer, the Kafka event outbox, and
-- a per-depot sync-state marker.

-- Documents captured at the depot and awaiting (or having completed) upstream
-- sync to the central DMS. This table IS the offline buffer: rows persist
-- locally regardless of connectivity and drain when the link is available.
CREATE TABLE IF NOT EXISTS depot_documents (
    id            UUID PRIMARY KEY,
    depot_id      TEXT NOT NULL,
    doc_type      TEXT NOT NULL,
    reference     TEXT NOT NULL DEFAULT '',
    outlet_id     TEXT NOT NULL DEFAULT '',
    payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    status        TEXT NOT NULL DEFAULT 'buffered'
                  CHECK (status IN ('buffered', 'syncing', 'synced', 'failed')),
    sync_attempts INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    upstream_id   TEXT,
    available_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    captured_by   TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    synced_at     TIMESTAMPTZ
);

-- Drives the sync claim query: pending rows whose backoff window has elapsed.
CREATE INDEX IF NOT EXISTS idx_depot_documents_pending
    ON depot_documents (available_at)
    WHERE status IN ('buffered', 'failed');

CREATE INDEX IF NOT EXISTS idx_depot_documents_depot_status
    ON depot_documents (depot_id, status);

-- Idempotent capture: a (depot, type, reference) triple is unique when a
-- reference is supplied, so a retried capture from the field does not double up.
CREATE UNIQUE INDEX IF NOT EXISTS uq_depot_documents_reference
    ON depot_documents (depot_id, doc_type, reference)
    WHERE reference <> '';

-- Transactional outbox for depot.* events published to iag.operations.
CREATE TABLE IF NOT EXISTS depot_event_outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_type    TEXT NOT NULL,
    event_key     TEXT,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    available_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    dispatched_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_depot_outbox_pending
    ON depot_event_outbox (available_at)
    WHERE dispatched_at IS NULL;

-- One row per depot recording the outcome of the most recent sync sweep.
CREATE TABLE IF NOT EXISTS depot_sync_state (
    depot_id     TEXT PRIMARY KEY,
    last_sync_at TIMESTAMPTZ,
    last_sync_ok BOOLEAN NOT NULL DEFAULT FALSE,
    last_error   TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
