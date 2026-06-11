INSERT INTO validation_results (check_name, source_table, issue_count, status)
SELECT
    'null_primary_ids_sessions',
    'src_network_sessions',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM src_network_sessions
WHERE session_id IS NULL OR customer_id IS NULL;
