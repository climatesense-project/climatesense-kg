SELECT
    step,
    COALESCE(split_part(payload->'data'->>'review_url', '/', 3), 'unknown') AS domain,
    COUNT(*) AS failure_count
FROM cache_entries
WHERE step LIKE 'enricher.%'
  AND (payload->>'success')::boolean = FALSE
  AND payload->'data' ? 'review_url'
  AND (:step IS NULL OR step = :step)
  AND (:from_ts IS NULL OR created_at >= :from_ts)
  AND (:to_ts IS NULL OR created_at <= :to_ts)
GROUP BY step, domain
ORDER BY failure_count DESC, domain ASC
LIMIT :limit;
