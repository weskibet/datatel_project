CREATE TABLE IF NOT EXISTS stg_billing (
    transaction_id   VARCHAR PRIMARY KEY,
    customer_id      VARCHAR NOT NULL,
    amount           NUMERIC NOT NULL DEFAULT 0,
    transaction_date TIMESTAMP NOT NULL
);

DO \$\$ DECLARE
    start_ts TIMESTAMP;
    end_ts   TIMESTAMP;
BEGIN
    SELECT (COALESCE(last_processed_date, '1970-01-01'::DATE)
            - '{{ params.lookback_days | default(3) }}'::INT * INTERVAL '1 day')::TIMESTAMP
    INTO start_ts
    FROM pipeline_watermarks
    WHERE source_name = 'billing';

    end_ts := '{{ ds }}'::DATE::TIMESTAMP + INTERVAL '1 day';

    DELETE FROM stg_billing
    WHERE transaction_date >= start_ts
      AND transaction_date <  end_ts;

    INSERT INTO stg_billing (transaction_id, customer_id, amount, transaction_date)
    SELECT
        transaction_id,
        customer_id,
        COALESCE(amount, 0) AS amount,
        transaction_date::TIMESTAMP AS transaction_date
    FROM (
        SELECT
            transaction_id,
            customer_id,
            amount,
            transaction_date,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_date DESC
            ) AS rn
        FROM src_billing_transactions
        WHERE transaction_id IS NOT NULL
          AND customer_id   IS NOT NULL
          AND transaction_date::TIMESTAMP >= start_ts
          AND transaction_date::TIMESTAMP <  end_ts
    ) deduped
    WHERE rn = 1;

    INSERT INTO pipeline_watermarks (source_name, last_processed_date, updated_at)
    VALUES ('billing', '{{ ds }}'::DATE, CURRENT_TIMESTAMP)
    ON CONFLICT (source_name)
    DO UPDATE SET last_processed_date = '{{ ds }}'::DATE,
                  updated_at = CURRENT_TIMESTAMP;
END \$\$;
