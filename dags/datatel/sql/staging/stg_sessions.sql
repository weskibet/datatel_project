CREATE TABLE IF NOT EXISTS stg_sessions (
    session_id           VARCHAR PRIMARY KEY,
    customer_id          VARCHAR NOT NULL,
    start_time           TIMESTAMP,
    end_time             TIMESTAMP,
    data_used_mb         NUMERIC NOT NULL DEFAULT 0,
    session_duration_sec INTEGER NOT NULL DEFAULT 0,
    session_date         TIMESTAMP NOT NULL
);

DO \$\$ DECLARE
    start_ts TIMESTAMP;
    end_ts   TIMESTAMP;
BEGIN
    SELECT (COALESCE(last_processed_date, '1970-01-01'::DATE)
            - '{{ params.lookback_days | default(3) }}'::INT * INTERVAL '1 day')::TIMESTAMP
    INTO start_ts
    FROM pipeline_watermarks
    WHERE source_name = 'sessions';

    end_ts := '{{ ds }}'::DATE::TIMESTAMP + INTERVAL '1 day';

    DELETE FROM stg_sessions
    WHERE session_date >= start_ts
      AND session_date <  end_ts;

    INSERT INTO stg_sessions (
        session_id, customer_id, start_time, end_time,
        data_used_mb, session_duration_sec, session_date
    )
    SELECT
        session_id,
        customer_id,
        start_time::TIMESTAMP,
        end_time::TIMESTAMP,
        COALESCE(data_used_mb, 0) AS data_used_mb,
        CASE
            WHEN end_time::TIMESTAMP > start_time::TIMESTAMP
            THEN EXTRACT(EPOCH FROM (end_time::TIMESTAMP - start_time::TIMESTAMP))::INTEGER
            ELSE 0
        END AS session_duration_sec,
        session_date::TIMESTAMP AS session_date
    FROM (
        SELECT
            session_id, customer_id, start_time, end_time,
            data_used_mb, session_date,
            ROW_NUMBER() OVER (
                PARTITION BY session_id
                ORDER BY session_date DESC
            ) AS rn
        FROM src_network_sessions
        WHERE session_id  IS NOT NULL
          AND customer_id IS NOT NULL
          AND session_date::TIMESTAMP >= start_ts
          AND session_date::TIMESTAMP <  end_ts
    ) deduped
    WHERE rn = 1;

    INSERT INTO pipeline_watermarks (source_name, last_processed_date, updated_at)
    VALUES ('sessions', '{{ ds }}'::DATE, CURRENT_TIMESTAMP)
    ON CONFLICT (source_name)
    DO UPDATE SET last_processed_date = '{{ ds }}'::DATE,
                  updated_at = CURRENT_TIMESTAMP;
END \$\$;
