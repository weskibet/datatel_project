WITH duplicate_ids AS (
    SELECT transaction_id
    FROM src_billing_transactions
    WHERE transaction_date::timestamptz >= %(lookback_start)s::timestamptz
      AND transaction_date::timestamptz < %(window_end)s::timestamptz
      AND transaction_id IS NOT NULL
    GROUP BY transaction_id
    HAVING count(*) > 1
),
bad_records AS (
    SELECT to_jsonb(b.*) AS record_json, 'billing_transactions' AS source
    FROM src_billing_transactions b
    JOIN duplicate_ids d USING (transaction_id)
),
inserted AS (
    INSERT INTO quarantine (record_json, source, detected_at)
    SELECT record_json, source, now()
    FROM bad_records
    RETURNING 1
)
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM duplicate_ids)
    THEN (SELECT count(*) FROM duplicate_ids)
    ELSE 1
END AS duplicate_transaction_id_count;
