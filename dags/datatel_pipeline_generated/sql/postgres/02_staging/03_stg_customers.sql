INSERT INTO stg_customers (
    customer_id,
    customer_name,
    email,
    country,
    customer_since,
    loaded_at
)
SELECT
    customer_id::text,
    initcap(trim(name)) AS customer_name,
    lower(trim(email)) AS email,
    coalesce(nullif(trim(country), ''), 'Nigeria') AS country,
    created_at::timestamptz AS customer_since,
    now() AS loaded_at
FROM (
    SELECT DISTINCT ON (customer_id)
        customer_id,
        name,
        email,
        country,
        created_at
    FROM src_customers
    WHERE customer_id IS NOT NULL
    ORDER BY customer_id, created_at::timestamptz DESC
) deduped_customers
WHERE customer_id IS NOT NULL
ON CONFLICT (customer_id) DO UPDATE
SET customer_name = EXCLUDED.customer_name,
    email = EXCLUDED.email,
    country = EXCLUDED.country,
    customer_since = EXCLUDED.customer_since,
    loaded_at = now();
