CREATE OR REPLACE TABLE {{ params.bq_project }}.{{ params.bq_staging_dataset }}.session_buckets AS
SELECT
    session_id,
    customer_id,
    session_duration_sec,
    CASE
        WHEN session_duration_sec < 60  THEN 'short'
        WHEN session_duration_sec < 300 THEN 'medium'
        ELSE 'long'
    END AS bucket
FROM {{ params.bq_project }}.{{ params.bq_staging_dataset }}.stg_sessions;
