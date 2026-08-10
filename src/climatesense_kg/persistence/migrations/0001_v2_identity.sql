CREATE TABLE source_review_records (
    record_key TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    native_id TEXT,
    observed_url TEXT NOT NULL,
    final_url TEXT,
    canonical_url TEXT,
    claim_uri TEXT NOT NULL,
    rating_fingerprint TEXT,
    source_text TEXT,
    extracted_text TEXT,
    normalized_text_hash TEXT,
    shingle_signature JSONB NOT NULL DEFAULT '[]'::jsonb,
    word_count INTEGER NOT NULL DEFAULT 0 CHECK (word_count >= 0),
    payload_hash TEXT NOT NULL,
    document_id UUID,
    claim_review_id UUID,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX source_review_native_id_unique
    ON source_review_records (source_name, native_id)
    WHERE native_id IS NOT NULL;

CREATE TABLE review_documents (
    id UUID PRIMARY KEY,
    organization_uri TEXT NOT NULL,
    preferred_url TEXT NOT NULL,
    final_url TEXT,
    canonical_url TEXT,
    extracted_text TEXT,
    normalized_text_hash TEXT,
    shingle_signature JSONB NOT NULL DEFAULT '[]'::jsonb,
    word_count INTEGER NOT NULL DEFAULT 0 CHECK (word_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX review_documents_org_canonical_url
    ON review_documents (organization_uri, canonical_url)
    WHERE canonical_url IS NOT NULL;

CREATE INDEX review_documents_org_text_hash
    ON review_documents (organization_uri, normalized_text_hash)
    WHERE normalized_text_hash IS NOT NULL;

CREATE TABLE claim_review_identities (
    id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES review_documents(id) ON DELETE RESTRICT,
    organization_uri TEXT NOT NULL,
    claim_uri TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (document_id, claim_uri)
);

ALTER TABLE source_review_records
    ADD CONSTRAINT source_review_document_fk
    FOREIGN KEY (document_id) REFERENCES review_documents(id) ON DELETE RESTRICT,
    ADD CONSTRAINT source_review_identity_fk
    FOREIGN KEY (claim_review_id)
    REFERENCES claim_review_identities(id) ON DELETE RESTRICT;

CREATE INDEX source_review_records_document_variant
    ON source_review_records (
        document_id, word_count DESC, last_seen_at DESC, record_key
    )
    WHERE COALESCE(extracted_text, source_text) IS NOT NULL;

CREATE INDEX claim_review_identity_candidates
    ON claim_review_identities (organization_uri, claim_uri);

CREATE TABLE identity_candidates (
    source_record_key TEXT NOT NULL
        REFERENCES source_review_records(record_key) ON DELETE CASCADE,
    candidate_review_id UUID NOT NULL
        REFERENCES claim_review_identities(id) ON DELETE CASCADE,
    similarity DOUBLE PRECISION NOT NULL CHECK (
        similarity >= 0 AND similarity <= 1
    ),
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_record_key, candidate_review_id)
);

CREATE TABLE stage_results (
    subject_key TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    stage_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        subject_key,
        stage_name,
        stage_version,
        input_hash,
        config_hash
    )
);

CREATE INDEX stage_results_stage_success
    ON stage_results (stage_name, success);

CREATE INDEX stage_results_updated_at
    ON stage_results (updated_at);
