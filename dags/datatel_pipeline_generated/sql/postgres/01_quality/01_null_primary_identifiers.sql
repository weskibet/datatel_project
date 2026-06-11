WITH bad_records AS (
    SELECT to_jsonb(b.*) AS record_json, 'billing_transactions' AS source
    FROM src_billing_transactions b
    WHERE (b.transaction_date::timestamptz >= %(lookback_start)s::timestamptz)
      AND (b.transaction_date::timestamptz < %(window_end)s::timestamptz)
      AND (b.transaction_id IS NULL OR b.customer_id IS NULL)

    UNION ALL

    SELECT to_jsonb(s.*) AS record_json, 'network_sessions' AS source
    FROM src_network_sessions s
    WHERE (s.start_time::timestamptz >= %(lookback_start)s::timestamptz)
      AND (s.start_time::timestamptz < %(window_end)s::timestamptz)
      AND (s.session_id IS NULL OR s.customer_id IS NULL)

    UNION ALL

    SELECT to_jsonb(c.*) AS record_json, 'customers' AS source
    FROM src_customers c
    WHERE c.customer_id IS NULL
),
inserted AS (
    INSERT INTO quarantine (record_json, source, detected_at)
    SELECT record_json, source, now()
    FROM bad_records
    RETURNING 1
)
SELECT count(*) AS null_primary_identifier_count
FROM bad_records;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM src_billing_transactions b
        WHERE (b.transaction_date::timestamptz >= %(lookback_start)s::timestamptz)
          AND (b.transaction_date::timestamptz < %(window_end)s::timestamptz)
          AND (b.transaction_id IS NULL OR b.customer_id IS NULL)

        UNION ALL

        SELECT 1
        FROM src_network_sessions s
        WHERE (s.start_time::timestamptz >= %(lookback_start)s::timestamptz)
          AND (s.start_time::timestamptz < %(window_end)s::timestamptz)
          AND (s.session_id IS NULL OR s.customer_id IS NULL)

        UNION ALL

        SELECT 1
        FROM src_customers c
        WHERE c.customer_id IS NULL
    ) THEN
        RAISE EXCEPTION 'Quality gate failed: null primary identifiers detected.';
    END IF;
END $$;
