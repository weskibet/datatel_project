INSERT INTO quarantine (row_data, source, detected_at)

-- NULL-identifier billing rows
SELECT to_jsonb(t), 'billing_transactions', CURRENT_TIMESTAMP
FROM src_billing_transactions t
WHERE transaction_id IS NULL OR customer_id IS NULL

UNION ALL

-- NULL-identifier session rows
SELECT to_jsonb(s), 'network_sessions', CURRENT_TIMESTAMP
FROM src_network_sessions s
WHERE session_id IS NULL OR customer_id IS NULL

UNION ALL

-- Duplicate billing rows (all copies)
SELECT to_jsonb(t), 'billing_transactions', CURRENT_TIMESTAMP
FROM src_billing_transactions t
WHERE transaction_id IN (
    SELECT transaction_id
    FROM src_billing_transactions
    WHERE transaction_id IS NOT NULL
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
)

UNION ALL

-- Duplicate session rows (all copies)
SELECT to_jsonb(s), 'network_sessions', CURRENT_TIMESTAMP
FROM src_network_sessions s
WHERE session_id IN (
    SELECT session_id
    FROM src_network_sessions
    WHERE session_id IS NOT NULL
    GROUP BY session_id
    HAVING COUNT(*) > 1
);
