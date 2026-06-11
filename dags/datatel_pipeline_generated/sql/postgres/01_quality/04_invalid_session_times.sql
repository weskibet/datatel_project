WITH bad_records AS (
    SELECT to_jsonb(s.*) AS record_json, 'network_sessions' AS source
    FROM src_network_sessions s
    WHERE start_time::timestamptz >= %(lookback_start)s::timestamptz
      AND start_time::timestamptz < %(window_end)s::timestamptz
      AND end_time::timestamptz < start_time::timestamptz
),
inserted AS (
    INSERT INTO quarantine (record_json, source, detected_at)
    SELECT record_json, source, now()
    FROM bad_records
    RETURNING 1
)
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM bad_records)
    THEN (SELECT count(*) FROM bad_records)
    ELSE 1
END AS invalid_session_time_count;
