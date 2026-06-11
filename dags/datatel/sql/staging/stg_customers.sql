CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id  VARCHAR PRIMARY KEY,
    name         VARCHAR,
    email        VARCHAR,
    country      VARCHAR DEFAULT 'Nigeria',
    created_at   TIMESTAMP
);

TRUNCATE stg_customers;

INSERT INTO stg_customers (customer_id, name, email, country, created_at)
SELECT
    customer_id,
    INITCAP(name)              AS name,
    LOWER(email)               AS email,
    COALESCE(country, 'Nigeria') AS country,
    created_at::TIMESTAMP      AS created_at
FROM src_customers
WHERE customer_id IS NOT NULL;
