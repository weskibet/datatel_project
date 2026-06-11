# DataTel Warehouse Pipeline

SQL-first Airflow pipeline for consolidating DataTel billing, network session, and customer source data into a customer analytics warehouse.

## What Is Included

- Docker Compose setup for Airflow and PostgreSQL
- CSV source-table loader for Docker Postgres
- SQL quality checks, staging tables, and transformation aggregates
- Airflow DAG orchestration
- BigQuery final table load to `dw_user_analytics`

The tested implementation is in:

```text
dags/datatel_pipeline_generated/
```

## Local Setup

1. Copy `.env.example` to `.env`.
2. Place your Google service-account JSON in `credentials/`.
3. Start services:

```powershell
docker compose up -d
```

4. Load source CSVs into Docker Postgres:

```powershell
powershell -ExecutionPolicy Bypass -File "dags\datatel_pipeline_generated\scripts\load_source_csvs_docker.ps1"
```

5. Open Airflow:

```text
http://localhost:8080
```

Default local login from the compose setup:

```text
username: admin
password: admin
```

## Airflow DAG

Use:

```text
datatel_daily_pipeline_generated
```

The DAG has been tested end to end with the provided CSVs, Docker Postgres, and BigQuery dataset:

```text
lucid-splicer-440817-u6.datatel_dw.dw_user_analytics
```

## Do Not Commit

The repo intentionally ignores:

- `.env`
- `credentials/`
- `data/*.csv`
- `logs/`
- Python cache files

These contain secrets, large generated data, or runtime output.
