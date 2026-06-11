CREATE TABLE IF NOT EXISTS quarantine (
    record_json jsonb NOT NULL,
    source text NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS etl_watermarks (
    pipeline_name text PRIMARY KEY,
    loaded_until timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stg_billing (
    transaction_id text PRIMARY KEY,
    customer_id text NOT NULL,
    amount numeric(18, 2) NOT NULL,
    transaction_ts timestamptz NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stg_sessions (
    session_id text PRIMARY KEY,
    customer_id text NOT NULL,
    start_ts timestamptz NOT NULL,
    end_ts timestamptz NOT NULL,
    session_date date,
    data_used_mb numeric(18, 2) NOT NULL,
    session_duration_sec integer NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id text PRIMARY KEY,
    customer_name text NOT NULL,
    email text,
    country text NOT NULL,
    customer_since timestamptz,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agg_user_revenue (
    customer_id text PRIMARY KEY,
    total_revenue numeric(18, 2) NOT NULL,
    total_transactions integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agg_user_usage (
    customer_id text PRIMARY KEY,
    total_data_used_mb numeric(18, 2) NOT NULL,
    avg_session_duration_sec numeric(18, 2) NOT NULL,
    total_sessions integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agg_monthly_revenue (
    customer_id text NOT NULL,
    revenue_month date NOT NULL,
    total_revenue numeric(18, 2) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, revenue_month)
);

CREATE TABLE IF NOT EXISTS agg_arpu (
    customer_id text PRIMARY KEY,
    arpu numeric(18, 2) NOT NULL,
    active_revenue_months integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS session_buckets (
    session_id text PRIMARY KEY,
    customer_id text NOT NULL,
    session_bucket text NOT NULL CHECK (session_bucket IN ('short', 'medium', 'long')),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agg_session_distribution (
    customer_id text PRIMARY KEY,
    short_sessions integer NOT NULL,
    medium_sessions integer NOT NULL,
    long_sessions integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stg_billing_transaction_ts ON stg_billing (transaction_ts);
CREATE INDEX IF NOT EXISTS idx_stg_billing_customer ON stg_billing (customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_sessions_start_ts ON stg_sessions (start_ts);
CREATE INDEX IF NOT EXISTS idx_stg_sessions_customer ON stg_sessions (customer_id);
