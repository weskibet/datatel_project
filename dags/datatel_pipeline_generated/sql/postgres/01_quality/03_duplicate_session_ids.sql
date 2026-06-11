WITH duplicate_ids AS (
    SELECT session_id
    FROM src_network_sessions
    WHERE start_time::timestamptz >= %(lookback_start)s::timestamptz
      AND start_time::timestamptz < %(window_end)s::timestamptz
      AND session_id IS NOT NULL
    GROUP BY session_id
    HAVING count(*) > 1
),
bad_records AS (
    SELECT to_jsonb(s.*) AS record_json, 'network_sessions' AS source
    FROM src_network_sessions s
    JOIN duplicate_ids d USING (session_id)
),
inserted AS (
    INSERT INTO quarantine (record_json, source, detected_at)
    SELECT record_json, source, now()
    FROM bad_records
    RETURNING 1
)
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM duplicate_ids)
    THEN (SELECT count(*) FROM duplicate_ids)
    ELSE 1
END AS duplicate_session_id_count;
