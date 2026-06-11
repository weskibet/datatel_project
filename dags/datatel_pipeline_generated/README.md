# DataTel Communications Warehouse Pipeline

This project implements the DataTel capstone as a SQL-first Airflow pipeline.

## Project Layout

```text
datatel_pipeline/
  dags/
    datatel_daily_pipeline.py
  scripts/
    load_source_csvs.ps1
  sql/
    postgres/
      00_schema/
      01_quality/
      02_staging/
      03_transform/
    bigquery/
  docs/
    discussion_answers.md
```

## Runtime Assumptions

- Source tables already exist in PostgreSQL:
  - `src_billing_transactions`
  - `src_network_sessions`
  - `src_customers`
- Airflow has the Postgres provider and Google provider installed.
- BigQuery has a connection resource that can query PostgreSQL through `EXTERNAL_QUERY`.

## Airflow Configuration

Create these Airflow Variables:

| Variable | Example | Purpose |
| --- | --- | --- |
| `datatel_postgres_conn_id` | `datatel_postgres` | Airflow connection id for PostgreSQL |
| `datatel_gcp_conn_id` | `google_cloud_default` | Airflow connection id for GCP |
| `datatel_bq_project` | `my-gcp-project` | BigQuery project |
| `datatel_bq_dataset` | `datatel_dw` | BigQuery dataset |
| `datatel_bq_location` | `US` | BigQuery location |
| `datatel_bq_connection` | `projects/my-gcp-project/locations/US/connections/datatel_postgres` | BigQuery connection to PostgreSQL |

The DAG exposes these parameters in the Airflow UI:

| Parameter | Default | Purpose |
| --- | --- | --- |
| `window_start` | Airflow data interval start | Start of the processing window |
| `window_end` | Airflow data interval end | End of the processing window |
| `lookback_start` | `data_interval_start - lookback_days` | Earlier boundary used to catch late records |
| `lookback_days` | `2` | Lookback period for late arrivals |

## How To Run

1. Copy `datatel_pipeline/dags/datatel_daily_pipeline.py` and the `sql` folder into the same Airflow project folder.
2. Configure the Airflow Variables listed above.
3. Ensure the PostgreSQL source tables are populated.
4. Trigger `datatel_daily_pipeline`.

## Loading The Provided CSVs

The three provided CSVs can be loaded into PostgreSQL with:

```powershell
$env:DATATEL_POSTGRES_URI = "postgresql://user:password@localhost:5432/datatel"
.\scripts\load_source_csvs.ps1
```

The loader creates the three `src_*` tables, truncates them, imports the CSVs with `psql \copy`, and prints row counts.

If Postgres is running in Docker and the container is named `postgres`, use:

```powershell
.\scripts\load_source_csvs_docker.ps1
```

This creates a separate `datatel` database inside the container, copies the CSVs into the container, imports them, and prints row counts.

## Deliverables

### Stage 1 - Data Quality Checks

The validation SQL files are in `sql/postgres/01_quality`.

1. `01_null_primary_identifiers.sql` checks for missing `transaction_id`, `session_id`, or `customer_id`. Missing identifiers prevent reliable joins, deduplication, and customer-level metrics.
2. `02_duplicate_transaction_ids.sql` checks duplicate billing transaction ids. If not blocked or resolved, revenue can be double-counted.
3. `03_duplicate_session_ids.sql` checks duplicate network session ids. If not blocked or resolved, usage and session counts can be inflated.
4. `04_invalid_session_times.sql` checks sessions where `end_time` is earlier than `start_time`. If not handled, duration metrics become negative and behavior buckets become misleading.

Bad rows are written to `quarantine(record_json, source, detected_at)`. Missing identifiers are treated as blocking failures. Duplicate billing/session identifiers and invalid session times are quarantined and reported, then handled by deterministic staging cleanup because the provided source files intentionally include retry duplicates and clock-sync errors.

### Stage 2 - Staging Layer

The staging SQL files are in `sql/postgres/02_staging`.

- `stg_billing` deduplicates with `DISTINCT ON (transaction_id)` and keeps the row with the latest `transaction_date`. This matches the retry-event problem: later copies are most likely to represent the final source state. It is more deterministic than keeping an arbitrary row and safer than summing duplicates, which would overstate revenue.
- `stg_sessions` casts timestamps, fills missing data usage with zero, and calculates `session_duration_sec`, using zero when the session end is not later than the start.
- `stg_customers` deduplicates repeated `customer_id` values by keeping the latest `created_at`, standardizes names with `initcap`, lowercases email, fills missing country with `Nigeria`, and casts `created_at`.

All staging loads use upsert patterns, making reruns idempotent.

### Stage 3 - Transformation Layer

The transformation SQL files are in `sql/postgres/03_transform`.

- `agg_user_revenue`
- `agg_user_usage`
- `agg_monthly_revenue`
- `agg_arpu`
- `session_buckets`
- `agg_session_distribution`

For ARPU, the denominator is the number of distinct revenue months. The SQL uses `nullif(active_revenue_months, 0)` and `coalesce(..., 0)` to avoid division by zero. Customers without transactions get ARPU `0`, which is analytically safer than returning NULL for dashboards.

### Stage 4 - Data Warehouse Table

The BigQuery SQL files are in `sql/bigquery`.

`dw_user_analytics` is loaded to BigQuery by Airflow from the Postgres warehouse query. In a billing-enabled BigQuery project, the included SQL design supports a `MERGE` pattern for new and returning customers. For this local Docker/Airflow setup, the generated DAG uses a BigQuery load job with `WRITE_TRUNCATE` because the connected GCP project has billing disabled and BigQuery blocks DML queries such as `MERGE` on free-tier projects.

The source query starts from `stg_customers` and left joins all metric tables. That join strategy guarantees every known customer appears even when they have no billing or session records. Missing metrics default to zero with `coalesce`.

### Stage 5 - Airflow Orchestration

The DAG is in `dags/datatel_daily_pipeline.py`.

The dependency graph lets independent checks run in parallel, then loads source-specific staging tables only after their quality gates pass. Billing and session aggregates run in parallel after their own staging tables complete. BigQuery runs only after customer, revenue, usage, ARPU, and session distribution outputs are ready.
