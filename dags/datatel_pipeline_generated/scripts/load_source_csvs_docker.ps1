param(
    [string]$ContainerName = "postgres",
    [string]$Database = "datatel",
    [string]$User = "airflow",
    [string]$DataDir = "C:\Users\LG Gram\Documents\DE_Alt_School\third-semester-project\datatel_project\data"
)

$ErrorActionPreference = "Stop"

$exists = docker exec $ContainerName psql -U $User -d airflow -tAc "SELECT 1 FROM pg_database WHERE datname = '$Database';"
if (($exists -join "").Trim() -ne "1") {
    docker exec $ContainerName createdb -U $User $Database
}

$containerDir = "/tmp/datatel_csvs"
docker exec $ContainerName mkdir -p $containerDir

docker cp (Join-Path $DataDir "src_billing_transactions.csv") "${ContainerName}:${containerDir}/src_billing_transactions.csv"
docker cp (Join-Path $DataDir "src_network_sessions.csv") "${ContainerName}:${containerDir}/src_network_sessions.csv"
docker cp (Join-Path $DataDir "src_customers.csv") "${ContainerName}:${containerDir}/src_customers.csv"
docker cp (Join-Path $PSScriptRoot "..\sql\postgres\00_schema\01_source_tables.sql") "${ContainerName}:${containerDir}/01_source_tables.sql"

docker exec $ContainerName psql -U $User -d $Database -v ON_ERROR_STOP=1 -f "${containerDir}/01_source_tables.sql"
docker exec $ContainerName psql -U $User -d $Database -v ON_ERROR_STOP=1 -c "TRUNCATE src_billing_transactions, src_network_sessions, src_customers;"

docker exec $ContainerName psql -U $User -d $Database -v ON_ERROR_STOP=1 -c "\copy src_billing_transactions(transaction_id, customer_id, amount, currency, transaction_date) FROM '${containerDir}/src_billing_transactions.csv' WITH (FORMAT csv, HEADER true)"
docker exec $ContainerName psql -U $User -d $Database -v ON_ERROR_STOP=1 -c "\copy src_network_sessions(session_id, customer_id, start_time, end_time, data_used_mb) FROM '${containerDir}/src_network_sessions.csv' WITH (FORMAT csv, HEADER true)"
docker exec $ContainerName psql -U $User -d $Database -v ON_ERROR_STOP=1 -c "\copy src_customers(customer_id, name, email, country, created_at) FROM '${containerDir}/src_customers.csv' WITH (FORMAT csv, HEADER true)"

docker exec $ContainerName psql -U $User -d $Database -v ON_ERROR_STOP=1 -c "SELECT 'src_billing_transactions' AS table_name, count(*) AS rows FROM src_billing_transactions UNION ALL SELECT 'src_network_sessions', count(*) FROM src_network_sessions UNION ALL SELECT 'src_customers', count(*) FROM src_customers;"
