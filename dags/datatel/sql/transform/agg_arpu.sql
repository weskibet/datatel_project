CREATE OR REPLACE TABLE {{ params.bq_project }}.{{ params.bq_staging_dataset }}.agg_arpu AS
WITH customer_revenue AS (
    SELECT
        customer_id,
        SUM(amount) AS total_revenue
    FROM {{ params.bq_project }}.{{ params.bq_staging_dataset }}.stg_billing
    GROUP BY customer_id
),
customer_active_months AS (
    SELECT
        customer_id,
        COUNT(DISTINCT DATE_TRUNC(transaction_date, MONTH)) AS active_months
    FROM {{ params.bq_project }}.{{ params.bq_staging_dataset }}.stg_billing
    GROUP BY customer_id
)
SELECT
    r.customer_id,
    CASE
        WHEN m.active_months = 0 OR m.active_months IS NULL THEN 0
        ELSE r.total_revenue / m.active_months
    END AS arpu
FROM customer_revenue r
JOIN customer_active_months m ON r.customer_id = m.customer_id

UNION ALL

SELECT
    c.customer_id,
    0 AS arpu
FROM {{ params.bq_project }}.{{ params.bq_staging_dataset }}.stg_customers c
WHERE c.customer_id NOT IN (SELECT customer_id FROM customer_revenue);
