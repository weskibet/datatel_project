INSERT INTO stg_billing (
    transaction_id,
    customer_id,
    amount,
    transaction_ts,
    loaded_at
)
SELECT DISTINCT ON (transaction_id)
    transaction_id::text,
    customer_id::text,
    coalesce(amount, 0)::numeric(18, 2) AS amount,
    transaction_date::timestamptz AS transaction_ts,
    now() AS loaded_at
FROM src_billing_transactions
WHERE transaction_date::timestamptz >= %(lookback_start)s::timestamptz
  AND transaction_date::timestamptz < %(window_end)s::timestamptz
  AND transaction_id IS NOT NULL
  AND customer_id IS NOT NULL
ORDER BY transaction_id, transaction_date::timestamptz DESC
ON CONFLICT (transaction_id) DO UPDATE
SET customer_id = EXCLUDED.customer_id,
    amount = EXCLUDED.amount,
    transaction_ts = EXCLUDED.transaction_ts,
    loaded_at = now();
