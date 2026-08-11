SELECT
    stage_name,
    stage_version,
    COUNT(*) AS total_results,
    COUNT(*) FILTER (WHERE success) AS successful,
    COUNT(*) FILTER (WHERE NOT success) AS failed,
    ROUND(
        COUNT(*) FILTER (WHERE success) * 100.0
        / NULLIF(COUNT(*), 0),
        2
    ) AS success_rate_percent
FROM stage_result_attempts
WHERE (:stage_name IS NULL OR stage_name = :stage_name)
  AND (:from_ts IS NULL OR created_at >= :from_ts)
  AND (:to_ts IS NULL OR created_at <= :to_ts)
GROUP BY stage_name, stage_version
ORDER BY success_rate_percent DESC, stage_name, stage_version;
