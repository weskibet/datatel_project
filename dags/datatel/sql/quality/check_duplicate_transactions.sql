INSERT INTO validation_results (check_name, source_table, issue_count, status)
SELECT
    'duplicate_transaction_ids',
    'src_billing_transactions',
    COUNT(*) - COUNT(DISTINCT transaction_id),
    CASE WHEN COUNT(*) = COUNT(DISTINCT transaction_id) THEN 'PASS' ELSE 'FAIL' END
FROM src_billing_transactions
WHERE transaction_id IS NOT NULL;
