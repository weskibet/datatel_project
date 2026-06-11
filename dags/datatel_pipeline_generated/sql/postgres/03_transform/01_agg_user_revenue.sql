WITH affected_customers AS (
    SELECT DISTINCT customer_id
    FROM stg_billing
    WHERE transaction_ts >= %(lookback_start)s::timestamptz
      AND transaction_ts < %(window_end)s::timestamptz
),
recalculated AS (
    SELECT
        b.customer_id,
        sum(b.amount)::numeric(18, 2) AS total_revenue,
        count(*)::integer AS total_transactions
    FROM stg_billing b
    JOIN affected_customers a USING (customer_id)
    GROUP BY b.customer_id
)
INSERT INTO agg_user_revenue (
    customer_id,
    total_revenue,
    total_transactions,
    updated_at
)
SELECT customer_id, total_revenue, total_transactions, now()
FROM recalculated
ON CONFLICT (customer_id) DO UPDATE
SET total_revenue = EXCLUDED.total_revenue,
    total_transactions = EXCLUDED.total_transactions,
    updated_at = now();
