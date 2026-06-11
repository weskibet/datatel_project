WITH affected_customers AS (
    SELECT DISTINCT customer_id
    FROM stg_sessions
    WHERE start_ts >= %(lookback_start)s::timestamptz
      AND start_ts < %(window_end)s::timestamptz
),
recalculated AS (
    SELECT
        a.customer_id,
        count(*) FILTER (WHERE b.session_bucket = 'short')::integer AS short_sessions,
        count(*) FILTER (WHERE b.session_bucket = 'medium')::integer AS medium_sessions,
        count(*) FILTER (WHERE b.session_bucket = 'long')::integer AS long_sessions
    FROM affected_customers a
    LEFT JOIN session_buckets b USING (customer_id)
    GROUP BY a.customer_id
)
INSERT INTO agg_session_distribution (
    customer_id,
    short_sessions,
    medium_sessions,
    long_sessions,
    updated_at
)
SELECT customer_id, short_sessions, medium_sessions, long_sessions, now()
FROM recalculated
ON CONFLICT (customer_id) DO UPDATE
SET short_sessions = EXCLUDED.short_sessions,
    medium_sessions = EXCLUDED.medium_sessions,
    long_sessions = EXCLUDED.long_sessions,
    updated_at = now();
