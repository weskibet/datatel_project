CREATE OR REPLACE TABLE {{ params.bq_project }}.{{ params.bq_staging_dataset }}.agg_user_revenue AS
SELECT
    customer_id,
    SUM(amount)   AS total_revenue,
    COUNT(*)      AS total_transactions
FROM {{ params.bq_project }}.{{ params.bq_staging_dataset }}.stg_billing
GROUP BY customer_id;
