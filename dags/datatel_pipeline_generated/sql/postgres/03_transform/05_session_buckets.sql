INSERT INTO session_buckets (
    session_id,
    customer_id,
    session_bucket,
    updated_at
)
SELECT
    session_id,
    customer_id,
    CASE
        WHEN session_duration_sec < 60 THEN 'short'
        WHEN session_duration_sec < 300 THEN 'medium'
        ELSE 'long'
    END AS session_bucket,
    now()
FROM stg_sessions
WHERE start_ts >= %(lookback_start)s::timestamptz
  AND start_ts < %(window_end)s::timestamptz
ON CONFLICT (session_id) DO UPDATE
SET customer_id = EXCLUDED.customer_id,
    session_bucket = EXCLUDED.session_bucket,
    updated_at = now();
