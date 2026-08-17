-- Authoritative pipeline state for ingestion, extraction, identity, and enrichment.

CREATE TABLE pipeline_runs (
    id UUID PRIMARY KEY,
    config_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'complete', 'failed')),
    error TEXT,
    summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ
);

CREATE INDEX pipeline_runs_started_at ON pipeline_runs (started_at DESC);

CREATE TABLE documents (
    id UUID PRIMARY KEY,
    organization_uri TEXT NOT NULL,
    preferred_url TEXT NOT NULL,
    preferred_url_rank SMALLINT NOT NULL DEFAULT 2 CHECK (
        preferred_url_rank BETWEEN 0 AND 2
    ),
    content TEXT,
    normalized_text_hash TEXT,
    word_count INTEGER NOT NULL DEFAULT 0 CHECK (word_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE document_urls (
    organization_uri TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    url TEXT NOT NULL,
    document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (organization_uri, normalized_url)
);

CREATE INDEX document_urls_document_id ON document_urls (document_id);

CREATE TABLE document_text_hashes (
    organization_uri TEXT NOT NULL,
    normalized_text_hash TEXT NOT NULL,
    document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (organization_uri, normalized_text_hash)
);

CREATE INDEX document_text_hashes_document_id
    ON document_text_hashes (document_id);

CREATE TABLE claim_reviews (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES documents(id) ON DELETE RESTRICT,
    organization_uri TEXT NOT NULL,
    claim_uri TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (document_id, claim_uri)
);

CREATE INDEX claim_reviews_organization_claim
    ON claim_reviews (organization_uri, claim_uri);

CREATE TABLE source_observations (
    record_key TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    native_id TEXT,
    organization_uri TEXT NOT NULL,
    claim_uri TEXT NOT NULL,
    claim_text TEXT NOT NULL,
    observed_url TEXT NOT NULL,
    document_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload JSONB NOT NULL,
    claim_review_id UUID REFERENCES claim_reviews(id) ON DELETE RESTRICT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE RESTRICT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX source_observations_native_id
    ON source_observations (source_name, native_id)
    WHERE native_id IS NOT NULL;

CREATE INDEX source_observations_active_document
    ON source_observations (document_key, record_key)
    WHERE active;

CREATE INDEX source_observations_active_review
    ON source_observations (claim_review_id, record_key)
    WHERE active AND claim_review_id IS NOT NULL;

CREATE INDEX source_observations_active_claim
    ON source_observations (claim_uri, record_key)
    WHERE active;

CREATE INDEX source_observations_source_active
    ON source_observations (source_name, active, record_key);

CREATE TABLE document_extractions (
    document_key TEXT PRIMARY KEY,
    requested_url TEXT NOT NULL,
    final_url TEXT,
    canonical_url TEXT,
    content TEXT,
    normalized_text_hash TEXT,
    word_count INTEGER NOT NULL DEFAULT 0 CHECK (word_count >= 0),
    extractor_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('success', 'retryable_failure', 'permanent_failure')
    ),
    retry_at TIMESTAMPTZ CHECK (
        retry_at IS NULL OR status = 'retryable_failure'
    ),
    failure_category TEXT,
    http_status INTEGER,
    error_type TEXT,
    error_message TEXT,
    request_attempted BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX document_extractions_retry_at
    ON document_extractions (retry_at)
    WHERE status = 'retryable_failure';

CREATE INDEX document_extractions_status
    ON document_extractions (status);

CREATE TABLE enrichment_results (
    enricher TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    enricher_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('success', 'retryable_failure', 'permanent_failure')
    ),
    retry_at TIMESTAMPTZ CHECK (
        retry_at IS NULL OR status = 'retryable_failure'
    ),
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (enricher, subject_key)
);

CREATE INDEX enrichment_results_status
    ON enrichment_results (enricher, status);

CREATE INDEX enrichment_results_retry_at
    ON enrichment_results (retry_at)
    WHERE status = 'retryable_failure';

CREATE VIEW processing_results AS
SELECT
    'document.extract'::TEXT AS stage_name,
    extractor_version AS stage_version,
    status,
    retry_at,
    jsonb_strip_nulls(
        jsonb_build_object(
            'url', requested_url,
            'failure_category', failure_category,
            'http_status', http_status,
            'error_type', error_type,
            'error', error_message,
            'request_attempted', request_attempted
        )
    ) AS payload,
    updated_at
FROM document_extractions
UNION ALL
SELECT
    enricher AS stage_name,
    enricher_version AS stage_version,
    status,
    retry_at,
    payload,
    updated_at
FROM enrichment_results;

CREATE TABLE duplicate_candidates (
    left_review_id UUID NOT NULL REFERENCES claim_reviews(id) ON DELETE CASCADE,
    right_review_id UUID NOT NULL REFERENCES claim_reviews(id) ON DELETE CASCADE,
    similarity DOUBLE PRECISION NOT NULL CHECK (
        similarity >= 0 AND similarity <= 1
    ),
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (left_review_id, right_review_id),
    CHECK (left_review_id < right_review_id)
);
