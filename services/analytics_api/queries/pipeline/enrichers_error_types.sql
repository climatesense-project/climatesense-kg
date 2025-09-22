SELECT
    step,
    payload->'error'->>'type' AS error_type,
    COUNT(*) AS error_count
FROM cache_entries
WHERE step LIKE 'enricher.%'
  AND (payload->>'success')::boolean = FALSE
  AND (:step IS NULL OR step = :step)
  AND (:from_ts IS NULL OR created_at >= :from_ts)
  AND (:to_ts IS NULL OR created_at <= :to_ts)
GROUP BY step, error_type
ORDER BY error_count DESC, step ASC
LIMIT :limit;
