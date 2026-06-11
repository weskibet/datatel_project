INSERT INTO validation_results (check_name, source_table, issue_count, status)
SELECT
    'duplicate_session_ids',
    'src_network_sessions',
    COUNT(*) - COUNT(DISTINCT session_id),
    CASE WHEN COUNT(*) = COUNT(DISTINCT session_id) THEN 'PASS' ELSE 'FAIL' END
FROM src_network_sessions
WHERE session_id IS NOT NULL;
