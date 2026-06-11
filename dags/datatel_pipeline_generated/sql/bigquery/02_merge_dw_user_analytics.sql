MERGE `{{ params.bq_project }}.{{ params.bq_dataset }}.dw_user_analytics` AS target
USING (
    SELECT
        customer_id,
        customer_name,
        email,
        country,
        customer_since,
        total_revenue,
        total_transactions,
        total_data_used_mb,
        avg_session_duration_sec,
        total_sessions,
        arpu,
        short_sessions,
        medium_sessions,
        long_sessions,
        avg_data_per_session_mb,
        CURRENT_TIMESTAMP() AS refreshed_at
    FROM EXTERNAL_QUERY(
        '{{ params.bq_connection }}',
        '''
        SELECT
            c.customer_id,
            c.customer_name,
            c.email,
            c.country,
            c.customer_since,
            coalesce(r.total_revenue, 0) AS total_revenue,
            coalesce(r.total_transactions, 0) AS total_transactions,
            coalesce(u.total_data_used_mb, 0) AS total_data_used_mb,
            coalesce(u.avg_session_duration_sec, 0) AS avg_session_duration_sec,
            coalesce(u.total_sessions, 0) AS total_sessions,
            coalesce(a.arpu, 0) AS arpu,
            coalesce(d.short_sessions, 0) AS short_sessions,
            coalesce(d.medium_sessions, 0) AS medium_sessions,
            coalesce(d.long_sessions, 0) AS long_sessions,
            coalesce(u.total_data_used_mb / nullif(u.total_sessions, 0), 0) AS avg_data_per_session_mb
        FROM stg_customers c
        LEFT JOIN agg_user_revenue r ON r.customer_id = c.customer_id
        LEFT JOIN agg_user_usage u ON u.customer_id = c.customer_id
        LEFT JOIN agg_arpu a ON a.customer_id = c.customer_id
        LEFT JOIN agg_session_distribution d ON d.customer_id = c.customer_id
        '''
    )
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    customer_name = source.customer_name,
    email = source.email,
    country = source.country,
    customer_since = source.customer_since,
    total_revenue = source.total_revenue,
    total_transactions = source.total_transactions,
    total_data_used_mb = source.total_data_used_mb,
    avg_session_duration_sec = source.avg_session_duration_sec,
    total_sessions = source.total_sessions,
    arpu = source.arpu,
    short_sessions = source.short_sessions,
    medium_sessions = source.medium_sessions,
    long_sessions = source.long_sessions,
    avg_data_per_session_mb = source.avg_data_per_session_mb,
    refreshed_at = source.refreshed_at
WHEN NOT MATCHED THEN INSERT (
    customer_id,
    customer_name,
    email,
    country,
    customer_since,
    total_revenue,
    total_transactions,
    total_data_used_mb,
    avg_session_duration_sec,
    total_sessions,
    arpu,
    short_sessions,
    medium_sessions,
    long_sessions,
    avg_data_per_session_mb,
    refreshed_at
) VALUES (
    source.customer_id,
    source.customer_name,
    source.email,
    source.country,
    source.customer_since,
    source.total_revenue,
    source.total_transactions,
    source.total_data_used_mb,
    source.avg_session_duration_sec,
    source.total_sessions,
    source.arpu,
    source.short_sessions,
    source.medium_sessions,
    source.long_sessions,
    source.avg_data_per_session_mb,
    source.refreshed_at
);
