# DataTel Communications Data Warehouse Pipeline

DataTel Communications is a telecom operator with operational data spread across billing, network sessions, and CRM customer records. This project builds a SQL-first data engineering pipeline that validates, cleans, transforms, and consolidates those sources into a BigQuery analytics table for customer analytics, churn-risk analysis, and revenue operations.

The pipeline is orchestrated with Apache Airflow, uses PostgreSQL for source, staging, and transformation layers, and writes the final warehouse output to BigQuery.

## Business Goals

- Identify high-value customers from revenue and usage behavior.
- Detect customers at risk of churn based on low activity and low revenue.
- Surface revenue and network-usage mismatches for operations teams.
- Provide analysts with one reliable customer-level warehouse table.

## Architecture

```text
CSV Source Files
      |
      v
PostgreSQL Source Tables
      |
      v
Data Quality Checks + Quarantine
      |
      v
Staging Layer
      |
      v
Transformation Aggregates
      |
      v
BigQuery dw_user_analytics
```

## Pipeline Stages

| Stage | Purpose | Main Outputs |
| --- | --- | --- |
| Data quality | Detect missing identifiers, duplicates, and invalid session times | `quarantine` |
| Staging | Clean, cast, deduplicate, and standardize source data | `stg_billing`, `stg_sessions`, `stg_customers` |
| Transformation | Build customer revenue, usage, ARPU, monthly revenue, and session-distribution metrics | `agg_*`, `session_buckets` |
| Warehouse | Combine customer profile and metric tables into one analytics table | `dw_user_analytics` |
| Orchestration | Run the pipeline daily with configurable date windows | Airflow DAG |

## Repository Structure

```text
.
├── dags/
│   ├── datatel/                         # Earlier/local DAG version
│   └── datatel_pipeline_generated/      # Tested end-to-end implementation
│       ├── dags/
│       │   └── datatel_daily_pipeline.py
│       ├── docs/
│       │   ├── csv_profile.md
│       │   └── discussion_answers.md
│       ├── scripts/
│       │   ├── load_source_csvs.ps1
│       │   └── load_source_csvs_docker.ps1
│       └── sql/
│           ├── bigquery/
│           └── postgres/
├── docker-compose.yml
├── generate_data.py
├── plugins/
├── .env.example
└── README.md
```

The tested DAG is:

```text
datatel_daily_pipeline_generated
```

## Source Data Profile

The provided CSV files contain realistic data-quality issues.

| Source File | Rows | Key Issues |
| --- | ---: | --- |
| `src_billing_transactions.csv` | 1,530,000 | Missing amounts, missing currency values, duplicate transaction ids |
| `src_network_sessions.csv` | 3,060,000 | Missing data usage, duplicate session ids, invalid session end times |
| `src_customers.csv` | 101,000 | Missing country values, duplicate customer ids |

Full profiling notes are available in:

```text
dags/datatel_pipeline_generated/docs/csv_profile.md
```

## Data Quality Strategy

The pipeline separates detection from cleanup:

- Missing primary identifiers are treated as hard quality failures.
- Duplicate transaction and session ids are quarantined, reported, and deduplicated in staging.
- Invalid session durations are quarantined, reported, and corrected by setting non-positive durations to zero.
- Bad records are stored in `quarantine(record_json, source, detected_at)` for auditability.

This lets the pipeline remain transparent without allowing known source-system issues to corrupt analytics.

## Staging Logic

### `stg_billing`

- Deduplicates by `transaction_id`.
- Keeps the latest `transaction_date` for retry duplicates.
- Replaces missing `amount` with `0`.
- Casts `transaction_date` to timestamp.

### `stg_sessions`

- Deduplicates by `session_id`.
- Casts `start_time` and `end_time` to timestamp.
- Replaces missing `data_used_mb` with `0`.
- Derives `session_date` from `start_time`.
- Calculates `session_duration_sec`.

### `stg_customers`

- Deduplicates by `customer_id`.
- Keeps the latest `created_at`.
- Standardizes customer names.
- Lowercases email addresses.
- Fills missing country with `Nigeria`.

## Transformation Outputs

| Table | Description |
| --- | --- |
| `agg_user_revenue` | Total revenue and transaction count per customer |
| `agg_user_usage` | Total data usage, average session duration, and session count |
| `agg_monthly_revenue` | Monthly revenue per customer |
| `agg_arpu` | Average revenue per active revenue month |
| `session_buckets` | Classifies sessions as short, medium, or long |
| `agg_session_distribution` | Counts short, medium, and long sessions per customer |

## Final Warehouse Table

The final table is:

```text
<your-gcp-project>.datatel_dw.dw_user_analytics
```

It contains one row per customer and includes:

- Customer profile fields
- Revenue metrics
- Usage metrics
- ARPU
- Session distribution
- Average data per session
- Refresh timestamp

## BigQuery Write Strategy

The design supports a `MERGE` pattern for production BigQuery projects with billing enabled. That pattern handles both new and returning customers without duplicating rows.

For this tested local setup, the connected GCP project does not have billing enabled, and BigQuery blocks DML statements such as `MERGE` in that state. To make the project runnable end to end, the DAG uses a BigQuery load job with `WRITE_TRUNCATE` for `dw_user_analytics`.

This keeps the final table idempotent for the capstone run while preserving the production `MERGE` SQL in:

```text
dags/datatel_pipeline_generated/sql/bigquery/
```

## Incremental Loading Strategy

The DAG exposes configurable date-window parameters:

| Parameter | Purpose |
| --- | --- |
| `window_start` | Start of the normal processing window |
| `window_end` | End of the processing window |
| `lookback_start` | Earlier start date used to capture late-arriving records |
| `lookback_days` | Number of days to look back |

Billing uses `transaction_date` as its event-time boundary. Sessions use `start_time`. Staging tables use upserts, so rerunning the same window does not duplicate records.

Customer records have no reliable activity timestamp, so `stg_customers` is loaded with a full idempotent upsert.

Detailed discussion answers are in:

```text
dags/datatel_pipeline_generated/docs/discussion_answers.md
```

## Local Setup

### 1. Configure Environment

Copy the example environment file:

```powershell
Copy-Item .env.example .env
```

Place your Google service-account JSON file in:

```text
credentials/
```

The real `.env` and credential files are intentionally ignored by Git.

### 2. Start Docker Services

```powershell
docker compose up -d
```

This starts:

- PostgreSQL
- Airflow webserver
- Airflow scheduler

### 3. Open Airflow

```text
http://localhost:8080
```

Default local development login:

```text
username: *****(as_you_wish)
password: *****(as_you_wish)
```

### 4. Load CSVs Into Docker Postgres

```powershell
powershell -ExecutionPolicy Bypass -File "dags\datatel_pipeline_generated\scripts\load_source_csvs_docker.ps1"
```

This creates a separate `datatel` database in the Postgres container, creates the three source tables, and loads the CSVs.

### 5. Configure Airflow Connection

Create a Postgres connection:

```text
Conn Id: datatel_postgres
Conn Type: Postgres
Host: postgres
Schema: datatel
Login: airflow
Password: airflow
Port: 5432
```

### 6. Configure Airflow Variables

```text
datatel_postgres_conn_id = datatel_postgres
datatel_bq_project = <your-gcp-project>
datatel_bq_dataset = datatel_dw
datatel_bq_location = US
```

Update the BigQuery project and dataset values if you use a different GCP project.

## Running The DAG

In Airflow, unpause and trigger:

```text
datatel_daily_pipeline_generated
```

For a full historical run, use this configuration:

```json
{
  "window_start": "2025-06-11T00:00:00+00:00",
  "lookback_start": "2025-06-11T00:00:00+00:00",
  "window_end": "2026-06-12T00:00:00+00:00",
  "lookback_days": 366
}
```

## Validation Results

The pipeline was tested end to end with Docker Postgres, Airflow, and BigQuery.

Postgres outputs:

| Table | Rows |
| --- | ---: |
| `stg_billing` | 1,500,000 |
| `stg_sessions` | 3,000,000 |
| `stg_customers` | 100,000 |
| `agg_user_revenue` | 91,044 |
| `agg_user_usage` | 97,621 |
| `agg_monthly_revenue` | 344,781 |
| `agg_arpu` | 91,044 |
| `session_buckets` | 3,000,000 |
| `agg_session_distribution` | 97,621 |
| `quarantine` | 241,447 |

BigQuery output:

| Table | Rows | Columns |
| --- | ---: | ---: |
| `<gcp-project>.datatel_dw.dw_user_analytics` | 100,000 | 16 |

## GitHub Notes

Do not commit service-account JSON files, raw source CSVs, Airflow logs, or local environment files.

## Key Deliverables

- Airflow DAG: `dags/datatel_pipeline_generated/dags/datatel_daily_pipeline.py`
- Postgres SQL: `dags/datatel_pipeline_generated/sql/postgres/`
- BigQuery SQL: `dags/datatel_pipeline_generated/sql/bigquery/`
- CSV profile: `dags/datatel_pipeline_generated/docs/csv_profile.md`
- Discussion answers: `dags/datatel_pipeline_generated/docs/discussion_answers.md`
