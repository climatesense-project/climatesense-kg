SELECT
    stage_name,
    stage_version,
    status,
    payload->>'failure_category' AS failure_category,
    CASE
        WHEN payload->>'http_status' ~ '^[0-9]+$'
        THEN (payload->>'http_status')::INTEGER
    END AS http_status,
    COUNT(*) AS result_count,
    MIN(retry_at) AS next_retry_at
FROM processing_results
WHERE status <> 'success'
  AND (:stage_name IS NULL OR stage_name = :stage_name)
  AND (:from_ts IS NULL OR updated_at >= :from_ts)
  AND (:to_ts IS NULL OR updated_at <= :to_ts)
GROUP BY stage_name, stage_version, status, failure_category, http_status
ORDER BY next_retry_at NULLS LAST, result_count DESC, stage_name, stage_version
LIMIT :limit;
