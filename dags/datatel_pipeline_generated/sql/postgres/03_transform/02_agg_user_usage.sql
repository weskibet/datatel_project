WITH affected_customers AS (
    SELECT DISTINCT customer_id
    FROM stg_sessions
    WHERE start_ts >= %(lookback_start)s::timestamptz
      AND start_ts < %(window_end)s::timestamptz
),
recalculated AS (
    SELECT
        s.customer_id,
        sum(s.data_used_mb)::numeric(18, 2) AS total_data_used_mb,
        avg(s.session_duration_sec)::numeric(18, 2) AS avg_session_duration_sec,
        count(*)::integer AS total_sessions
    FROM stg_sessions s
    JOIN affected_customers a USING (customer_id)
    GROUP BY s.customer_id
)
INSERT INTO agg_user_usage (
    customer_id,
    total_data_used_mb,
    avg_session_duration_sec,
    total_sessions,
    updated_at
)
SELECT customer_id, total_data_used_mb, avg_session_duration_sec, total_sessions, now()
FROM recalculated
ON CONFLICT (customer_id) DO UPDATE
SET total_data_used_mb = EXCLUDED.total_data_used_mb,
    avg_session_duration_sec = EXCLUDED.avg_session_duration_sec,
    total_sessions = EXCLUDED.total_sessions,
    updated_at = now();
