-- Recomputable semantic-stage results and immutable attempt diagnostics.
CREATE TABLE IF NOT EXISTS stage_results (
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

CREATE INDEX IF NOT EXISTS stage_results_stage_success
    ON stage_results (stage_name, success);

CREATE INDEX IF NOT EXISTS stage_results_updated_at
    ON stage_results (updated_at);

CREATE TABLE stage_result_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subject_key TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    stage_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    success BOOLEAN NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX stage_result_attempts_stage_success
    ON stage_result_attempts (stage_name, success);

CREATE INDEX stage_result_attempts_created_at
    ON stage_result_attempts (created_at);
