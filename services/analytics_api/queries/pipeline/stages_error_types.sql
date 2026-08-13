SELECT
    stage_name,
    stage_version,
    status,
    payload->>'error_type' AS error_type,
    payload->>'failure_category' AS failure_category,
    CASE
        WHEN payload->>'http_status' ~ '^[0-9]+$'
        THEN (payload->>'http_status')::INTEGER
    END AS http_status,
    COUNT(*) AS error_count
FROM stage_result_attempts
WHERE status <> 'success'
  AND (:stage_name IS NULL OR stage_name = :stage_name)
  AND (:from_ts IS NULL OR created_at >= :from_ts)
  AND (:to_ts IS NULL OR created_at <= :to_ts)
GROUP BY stage_name, stage_version, status, error_type, failure_category, http_status
ORDER BY error_count DESC, stage_name, stage_version, status
LIMIT :limit;
