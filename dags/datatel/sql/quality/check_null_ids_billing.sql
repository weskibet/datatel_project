INSERT INTO validation_results (check_name, source_table, issue_count, status)
SELECT
    'null_primary_ids_billing',
    'src_billing_transactions',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM src_billing_transactions
WHERE transaction_id IS NULL OR customer_id IS NULL;
