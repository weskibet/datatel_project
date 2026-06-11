CREATE TABLE IF NOT EXISTS `{{ params.bq_project }}.{{ params.bq_dataset }}.dw_user_analytics` (
    customer_id STRING NOT NULL,
    customer_name STRING,
    email STRING,
    country STRING,
    customer_since TIMESTAMP,
    total_revenue NUMERIC,
    total_transactions INT64,
    total_data_used_mb NUMERIC,
    avg_session_duration_sec NUMERIC,
    total_sessions INT64,
    arpu NUMERIC,
    short_sessions INT64,
    medium_sessions INT64,
    long_sessions INT64,
    avg_data_per_session_mb NUMERIC,
    refreshed_at TIMESTAMP
);
