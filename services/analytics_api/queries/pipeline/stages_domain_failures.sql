SELECT
    stage_name,
    stage_version,
    status,
    COALESCE(NULLIF(split_part(payload->>'url', '/', 3), ''), 'unknown') AS domain,
    COUNT(*) AS failure_count
FROM stage_result_attempts
WHERE stage_name = 'document.extract'
  AND status <> 'success'
  AND payload ? 'url'
  AND (:stage_name IS NULL OR stage_name = :stage_name)
  AND (:from_ts IS NULL OR created_at >= :from_ts)
  AND (:to_ts IS NULL OR created_at <= :to_ts)
GROUP BY stage_name, stage_version, status, domain
ORDER BY failure_count DESC, domain, status
LIMIT :limit;
