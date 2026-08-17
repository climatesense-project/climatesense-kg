SELECT
    stage_name,
    stage_version,
    COUNT(*) AS total_results,
    COUNT(*) FILTER (WHERE status = 'success') AS successful,
    COUNT(*) FILTER (WHERE status <> 'success') AS failed,
    ROUND(
        COUNT(*) FILTER (WHERE status = 'success') * 100.0
        / NULLIF(COUNT(*), 0),
        2
    ) AS success_rate_percent
FROM processing_results
WHERE (:stage_name IS NULL OR stage_name = :stage_name)
  AND (:from_ts IS NULL OR updated_at >= :from_ts)
  AND (:to_ts IS NULL OR updated_at <= :to_ts)
GROUP BY stage_name, stage_version
ORDER BY success_rate_percent DESC, stage_name, stage_version;
