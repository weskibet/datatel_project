CREATE OR REPLACE TABLE {{ params.bq_project }}.{{ params.bq_staging_dataset }}.agg_session_distribution AS
SELECT
    customer_id,
    COUNTIF(bucket = 'short')  AS short_sessions,
    COUNTIF(bucket = 'medium') AS medium_sessions,
    COUNTIF(bucket = 'long')   AS long_sessions
FROM {{ params.bq_project }}.{{ params.bq_staging_dataset }}.session_buckets
GROUP BY customer_id;
