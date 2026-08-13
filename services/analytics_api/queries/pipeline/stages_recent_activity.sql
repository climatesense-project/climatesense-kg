SELECT
    stage_name,
    stage_version,
    COUNT(*) AS recent_results,
    MIN(created_at) AS earliest,
    MAX(created_at) AS latest,
    COUNT(*) FILTER (WHERE status = 'success') AS successful,
    COUNT(*) FILTER (WHERE status <> 'success') AS failed
FROM stage_result_attempts
WHERE (:stage_name IS NULL OR stage_name = :stage_name)
  AND (:from_ts IS NULL OR created_at >= :from_ts)
  AND (:to_ts IS NULL OR created_at <= :to_ts)
GROUP BY stage_name, stage_version
ORDER BY recent_results DESC, stage_name, stage_version
LIMIT :limit;
