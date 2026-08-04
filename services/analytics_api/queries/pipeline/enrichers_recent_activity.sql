SELECT
    step,
    COUNT(*) AS recent_entries,
    MIN(updated_at) AS earliest,
    MAX(updated_at) AS latest,
    COUNT(*) FILTER (WHERE (payload->>'success')::boolean = TRUE) AS successful,
    COUNT(*) FILTER (WHERE (payload->>'success')::boolean = FALSE) AS failed
FROM cache_entries
WHERE step LIKE 'enricher.%'
  AND (:step IS NULL OR step = :step)
  AND (:from_ts IS NULL OR updated_at >= :from_ts)
  AND (:to_ts IS NULL OR updated_at <= :to_ts)
GROUP BY step
ORDER BY recent_entries DESC, step ASC
LIMIT :limit;
