CREATE OR REPLACE TABLE {{ params.bq_project }}.{{ params.bq_staging_dataset }}.agg_user_usage AS
SELECT
    customer_id,
    SUM(data_used_mb)                              AS total_data_used_mb,
    ROUND(AVG(session_duration_sec), 2)            AS avg_session_duration_sec,
    COUNT(*)                                       AS total_sessions
FROM {{ params.bq_project }}.{{ params.bq_staging_dataset }}.stg_sessions
GROUP BY customer_id;
