WITH affected_customers AS (
    SELECT DISTINCT customer_id
    FROM stg_billing
    WHERE transaction_ts >= %(lookback_start)s::timestamptz
      AND transaction_ts < %(window_end)s::timestamptz
),
recalculated AS (
    SELECT
        a.customer_id,
        coalesce(sum(m.total_revenue), 0)::numeric(18, 2) AS total_revenue,
        count(m.revenue_month)::integer AS active_revenue_months
    FROM affected_customers a
    LEFT JOIN agg_monthly_revenue m USING (customer_id)
    GROUP BY a.customer_id
)
INSERT INTO agg_arpu (
    customer_id,
    arpu,
    active_revenue_months,
    updated_at
)
SELECT
    customer_id,
    coalesce((total_revenue / nullif(active_revenue_months, 0))::numeric(18, 2), 0) AS arpu,
    active_revenue_months,
    now()
FROM recalculated
ON CONFLICT (customer_id) DO UPDATE
SET arpu = EXCLUDED.arpu,
    active_revenue_months = EXCLUDED.active_revenue_months,
    updated_at = now();
