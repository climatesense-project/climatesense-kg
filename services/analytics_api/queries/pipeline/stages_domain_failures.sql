SELECT
    stage_name,
    stage_version,
    COALESCE(NULLIF(split_part(payload->>'url', '/', 3), ''), 'unknown') AS domain,
    COUNT(*) AS failure_count
FROM stage_results
WHERE stage_name = 'document.extract'
  AND NOT success
  AND payload ? 'url'
  AND (:stage_name IS NULL OR stage_name = :stage_name)
  AND (:from_ts IS NULL OR updated_at >= :from_ts)
  AND (:to_ts IS NULL OR updated_at <= :to_ts)
GROUP BY stage_name, stage_version, domain
ORDER BY failure_count DESC, domain
LIMIT :limit;
