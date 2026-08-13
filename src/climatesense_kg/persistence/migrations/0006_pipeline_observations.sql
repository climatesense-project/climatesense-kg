-- Durable, run-scoped source observations keep pipeline memory bounded while
-- retaining source-level ingestion atomicity.
CREATE TABLE pipeline_runs (
    id UUID PRIMARY KEY,
    signature TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'complete', 'failed')),
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ
);

CREATE INDEX pipeline_runs_signature_created
    ON pipeline_runs (signature, created_at DESC);

CREATE TABLE ingestion_records (
    run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    source_name TEXT NOT NULL,
    position BIGINT NOT NULL CHECK (position >= 0),
    record_key TEXT NOT NULL,
    observed_url TEXT NOT NULL,
    document_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (run_id, source_name, position),
    UNIQUE (run_id, record_key)
);

CREATE INDEX ingestion_records_run_record
    ON ingestion_records (run_id, record_key);

CREATE INDEX ingestion_records_run_document
    ON ingestion_records (run_id, document_key);
