WITH affected_months AS (
    SELECT DISTINCT
        customer_id,
        date_trunc('month', transaction_ts)::date AS revenue_month
    FROM stg_billing
    WHERE transaction_ts >= %(lookback_start)s::timestamptz
      AND transaction_ts < %(window_end)s::timestamptz
),
recalculated AS (
    SELECT
        b.customer_id,
        date_trunc('month', b.transaction_ts)::date AS revenue_month,
        sum(b.amount)::numeric(18, 2) AS total_revenue
    FROM stg_billing b
    JOIN affected_months a
      ON a.customer_id = b.customer_id
     AND a.revenue_month = date_trunc('month', b.transaction_ts)::date
    GROUP BY b.customer_id, date_trunc('month', b.transaction_ts)::date
)
INSERT INTO agg_monthly_revenue (
    customer_id,
    revenue_month,
    total_revenue,
    updated_at
)
SELECT customer_id, revenue_month, total_revenue, now()
FROM recalculated
ON CONFLICT (customer_id, revenue_month) DO UPDATE
SET total_revenue = EXCLUDED.total_revenue,
    updated_at = now();
