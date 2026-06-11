CREATE TABLE IF NOT EXISTS {{ params.bq_project }}.{{ params.bq_warehouse_dataset }}.dw_user_analytics (
    customer_id             STRING,
    customer_name           STRING,
    email                   STRING,
    country                 STRING,
    customer_since          TIMESTAMP,
    total_revenue           NUMERIC DEFAULT 0,
    total_transactions      INTEGER DEFAULT 0,
    total_data_used_mb      NUMERIC DEFAULT 0,
    avg_session_duration_sec NUMERIC DEFAULT 0,
    total_sessions          INTEGER DEFAULT 0,
    arpu                    NUMERIC DEFAULT 0,
    short_sessions          INTEGER DEFAULT 0,
    medium_sessions         INTEGER DEFAULT 0,
    long_sessions           INTEGER DEFAULT 0,
    avg_data_per_session_mb NUMERIC DEFAULT 0
);

MERGE {{ params.bq_project }}.{{ params.bq_warehouse_dataset }}.dw_user_analytics T
USING (
    SELECT
        c.customer_id,
        c.name                  AS customer_name,
        c.email,
        c.country,
        c.created_at            AS customer_since,
        COALESCE(r.total_revenue, 0)          AS total_revenue,
        COALESCE(r.total_transactions, 0)      AS total_transactions,
        COALESCE(u.total_data_used_mb, 0)      AS total_data_used_mb,
        COALESCE(u.avg_session_duration_sec, 0) AS avg_session_duration_sec,
        COALESCE(u.total_sessions, 0)          AS total_sessions,
        COALESCE(a.arpu, 0)                    AS arpu,
        COALESCE(s.short_sessions, 0)          AS short_sessions,
        COALESCE(s.medium_sessions, 0)         AS medium_sessions,
        COALESCE(s.long_sessions, 0)           AS long_sessions,
        CASE
            WHEN COALESCE(u.total_sessions, 0) > 0
            THEN COALESCE(u.total_data_used_mb, 0) / u.total_sessions
            ELSE 0
        END AS avg_data_per_session_mb
    FROM {{ params.bq_project }}.{{ params.bq_staging_dataset }}.stg_customers c
    LEFT JOIN {{ params.bq_project }}.{{ params.bq_staging_dataset }}.agg_user_revenue r
        ON c.customer_id = r.customer_id
    LEFT JOIN {{ params.bq_project }}.{{ params.bq_staging_dataset }}.agg_user_usage u
        ON c.customer_id = u.customer_id
    LEFT JOIN {{ params.bq_project }}.{{ params.bq_staging_dataset }}.agg_arpu a
        ON c.customer_id = a.customer_id
    LEFT JOIN {{ params.bq_project }}.{{ params.bq_staging_dataset }}.agg_session_distribution s
        ON c.customer_id = s.customer_id
) S
ON T.customer_id = S.customer_id

WHEN MATCHED THEN
    UPDATE SET
        customer_name           = S.customer_name,
        email                   = S.email,
        country                 = S.country,
        customer_since          = S.customer_since,
        total_revenue           = S.total_revenue,
        total_transactions      = S.total_transactions,
        total_data_used_mb      = S.total_data_used_mb,
        avg_session_duration_sec = S.avg_session_duration_sec,
        total_sessions          = S.total_sessions,
        arpu                    = S.arpu,
        short_sessions          = S.short_sessions,
        medium_sessions         = S.medium_sessions,
        long_sessions           = S.long_sessions,
        avg_data_per_session_mb = S.avg_data_per_session_mb

WHEN NOT MATCHED THEN
    INSERT (
        customer_id, customer_name, email, country, customer_since,
        total_revenue, total_transactions, total_data_used_mb,
        avg_session_duration_sec, total_sessions, arpu,
        short_sessions, medium_sessions, long_sessions,
        avg_data_per_session_mb
    )
    VALUES (
        S.customer_id, S.customer_name, S.email, S.country, S.customer_since,
        S.total_revenue, S.total_transactions, S.total_data_used_mb,
        S.avg_session_duration_sec, S.total_sessions, S.arpu,
        S.short_sessions, S.medium_sessions, S.long_sessions,
        S.avg_data_per_session_mb
    );
