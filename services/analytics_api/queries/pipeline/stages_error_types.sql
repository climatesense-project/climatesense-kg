SELECT
    stage_name,
    stage_version,
    payload->>'error_type' AS error_type,
    COUNT(*) AS error_count
FROM stage_result_attempts
WHERE NOT success
  AND (:stage_name IS NULL OR stage_name = :stage_name)
  AND (:from_ts IS NULL OR created_at >= :from_ts)
  AND (:to_ts IS NULL OR created_at <= :to_ts)
GROUP BY stage_name, stage_version, error_type
ORDER BY error_count DESC, stage_name, stage_version
LIMIT :limit;
