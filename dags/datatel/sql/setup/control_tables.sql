CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    source_name   VARCHAR(50) PRIMARY KEY,
    last_processed_date DATE,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO pipeline_watermarks (source_name, last_processed_date)
VALUES
    ('billing',  '1970-01-01'),
    ('sessions', '1970-01-01'),
    ('customers','1970-01-01')
ON CONFLICT (source_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS validation_results (
    check_name    VARCHAR(100),
    source_table  VARCHAR(50),
    issue_count   INTEGER,
    check_date    DATE DEFAULT CURRENT_DATE,
    status        VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS quarantine (
    row_data     JSONB,
    source       VARCHAR(50),
    detected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
