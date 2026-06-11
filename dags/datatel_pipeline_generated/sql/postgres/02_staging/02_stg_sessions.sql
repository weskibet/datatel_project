INSERT INTO stg_sessions (
    session_id,
    customer_id,
    start_ts,
    end_ts,
    session_date,
    data_used_mb,
    session_duration_sec,
    loaded_at
)
SELECT DISTINCT ON (session_id)
    session_id::text,
    customer_id::text,
    start_time::timestamptz AS start_ts,
    end_time::timestamptz AS end_ts,
    start_time::date AS session_date,
    coalesce(data_used_mb, 0)::numeric(18, 2) AS data_used_mb,
    CASE
        WHEN end_time::timestamptz > start_time::timestamptz
        THEN extract(epoch FROM (end_time::timestamptz - start_time::timestamptz))::integer
        ELSE 0
    END AS session_duration_sec,
    now() AS loaded_at
FROM src_network_sessions
WHERE start_time::timestamptz >= %(lookback_start)s::timestamptz
  AND start_time::timestamptz < %(window_end)s::timestamptz
  AND session_id IS NOT NULL
  AND customer_id IS NOT NULL
ORDER BY session_id, start_time::timestamptz DESC
ON CONFLICT (session_id) DO UPDATE
SET customer_id = EXCLUDED.customer_id,
    start_ts = EXCLUDED.start_ts,
    end_ts = EXCLUDED.end_ts,
    session_date = EXCLUDED.session_date,
    data_used_mb = EXCLUDED.data_used_mb,
    session_duration_sec = EXCLUDED.session_duration_sec,
    loaded_at = now();
