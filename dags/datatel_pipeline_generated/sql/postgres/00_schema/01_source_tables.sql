CREATE TABLE IF NOT EXISTS src_billing_transactions (
    transaction_id text,
    customer_id text,
    amount numeric(18, 2),
    currency text,
    transaction_date text
);

CREATE TABLE IF NOT EXISTS src_network_sessions (
    session_id text,
    customer_id text,
    start_time text,
    end_time text,
    data_used_mb numeric(18, 2)
);

CREATE TABLE IF NOT EXISTS src_customers (
    customer_id text,
    name text,
    email text,
    country text,
    created_at text
);
