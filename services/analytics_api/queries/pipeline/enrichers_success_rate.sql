SELECT
    step,
    COUNT(*) AS total_entries,
    COUNT(*) FILTER (WHERE (payload->>'success')::boolean = TRUE) AS successful,
    COUNT(*) FILTER (WHERE (payload->>'success')::boolean = FALSE) AS failed,
    ROUND(
        COUNT(*) FILTER (WHERE (payload->>'success')::boolean = TRUE) * 100.0
        / NULLIF(COUNT(*), 0),
        2
    ) AS success_rate_percent
FROM cache_entries
WHERE step LIKE 'enricher.%'
  AND (:step IS NULL OR step = :step)
  AND (:from_ts IS NULL OR created_at >= :from_ts)
  AND (:to_ts IS NULL OR created_at <= :to_ts)
GROUP BY step
ORDER BY success_rate_percent DESC, step ASC;
