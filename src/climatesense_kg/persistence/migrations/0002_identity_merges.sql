-- Retired identities remain traceable after their canonical rows are consolidated.
-- merged_into_id records the original decision; survivor_id follows later merges.
CREATE TABLE document_merges (
    retired_id UUID PRIMARY KEY,
    survivor_id UUID NOT NULL REFERENCES documents(id) ON DELETE RESTRICT,
    merged_into_id UUID NOT NULL,
    run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE RESTRICT,
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (retired_id <> survivor_id AND retired_id <> merged_into_id)
);
CREATE INDEX document_merges_survivor ON document_merges (survivor_id);

CREATE TABLE claim_review_merges (
    retired_id UUID PRIMARY KEY,
    survivor_id UUID NOT NULL REFERENCES claim_reviews(id) ON DELETE RESTRICT,
    merged_into_id UUID NOT NULL,
    run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE RESTRICT,
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (retired_id <> survivor_id AND retired_id <> merged_into_id)
);
CREATE INDEX claim_review_merges_survivor ON claim_review_merges (survivor_id);

-- Reconciliation also follows inactive observations and historical reviews.
CREATE INDEX source_observations_review ON source_observations (claim_review_id);
CREATE INDEX source_observations_active_organization
    ON source_observations (organization_uri, record_key) WHERE active;
CREATE INDEX documents_organization ON documents (organization_uri);
CREATE INDEX claim_reviews_document ON claim_reviews (document_id);
