CREATE OR REPLACE TABLE {{ params.bq_project }}.{{ params.bq_staging_dataset }}.agg_monthly_revenue AS
SELECT
    customer_id,
    DATE_TRUNC(transaction_date, MONTH) AS revenue_month,
    SUM(amount)                         AS monthly_revenue
FROM {{ params.bq_project }}.{{ params.bq_staging_dataset }}.stg_billing
GROUP BY customer_id, DATE_TRUNC(transaction_date, MONTH);
