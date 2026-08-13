-- Recomputable semantic-stage results and immutable attempt diagnostics.
CREATE TABLE IF NOT EXISTS stage_results (
    subject_key TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    stage_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('success', 'retryable_failure', 'permanent_failure')
    ),
    retry_at TIMESTAMPTZ CHECK (
        retry_at IS NULL OR status = 'retryable_failure'
    ),
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

CREATE INDEX IF NOT EXISTS stage_results_stage_status
    ON stage_results (stage_name, status);

CREATE INDEX IF NOT EXISTS stage_results_retry_at
    ON stage_results (retry_at)
    WHERE status = 'retryable_failure';

CREATE INDEX IF NOT EXISTS stage_results_updated_at
    ON stage_results (updated_at);

CREATE TABLE stage_result_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subject_key TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    stage_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('success', 'retryable_failure', 'permanent_failure')
    ),
    retry_at TIMESTAMPTZ CHECK (
        retry_at IS NULL OR status = 'retryable_failure'
    ),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX stage_result_attempts_stage_status
    ON stage_result_attempts (stage_name, status);

CREATE INDEX stage_result_attempts_created_at
    ON stage_result_attempts (created_at);
