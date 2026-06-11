param(
    [string]$PostgresUri = $env:DATATEL_POSTGRES_URI,
    [string]$DataDir = "C:\Users\LG Gram\Documents\DE_Alt_School\third-semester-project\datatel_project\data"
)

if ([string]::IsNullOrWhiteSpace($PostgresUri)) {
    throw "Set DATATEL_POSTGRES_URI or pass -PostgresUri. Example: postgresql://user:password@localhost:5432/datatel"
}

$ErrorActionPreference = "Stop"

$schemaPath = Join-Path $PSScriptRoot "..\sql\postgres\00_schema\01_source_tables.sql"
psql $PostgresUri -v ON_ERROR_STOP=1 -f $schemaPath

psql $PostgresUri -v ON_ERROR_STOP=1 -c "TRUNCATE src_billing_transactions, src_network_sessions, src_customers;"

$billing = Join-Path $DataDir "src_billing_transactions.csv"
$sessions = Join-Path $DataDir "src_network_sessions.csv"
$customers = Join-Path $DataDir "src_customers.csv"

psql $PostgresUri -v ON_ERROR_STOP=1 -c "\copy src_billing_transactions(transaction_id, customer_id, amount, currency, transaction_date) FROM '$billing' WITH (FORMAT csv, HEADER true)"
psql $PostgresUri -v ON_ERROR_STOP=1 -c "\copy src_network_sessions(session_id, customer_id, start_time, end_time, data_used_mb) FROM '$sessions' WITH (FORMAT csv, HEADER true)"
psql $PostgresUri -v ON_ERROR_STOP=1 -c "\copy src_customers(customer_id, name, email, country, created_at) FROM '$customers' WITH (FORMAT csv, HEADER true)"

psql $PostgresUri -v ON_ERROR_STOP=1 -c "SELECT 'src_billing_transactions' AS table_name, count(*) AS rows FROM src_billing_transactions UNION ALL SELECT 'src_network_sessions', count(*) FROM src_network_sessions UNION ALL SELECT 'src_customers', count(*) FROM src_customers;"
