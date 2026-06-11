from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from google.cloud import bigquery


DAG_DIR = Path(__file__).resolve().parent
PROJECT_DIR = DAG_DIR.parent
POSTGRES_SQL_DIR = PROJECT_DIR / "sql" / "postgres"
BIGQUERY_SQL_DIR = PROJECT_DIR / "sql" / "bigquery"


def read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


POSTGRES_CONN_ID = Variable.get("datatel_postgres_conn_id", default_var="datatel_postgres")
GCP_CONN_ID = Variable.get("datatel_gcp_conn_id", default_var="google_cloud_default")
BQ_PROJECT = Variable.get("datatel_bq_project", default_var="your-gcp-project")
BQ_DATASET = Variable.get("datatel_bq_dataset", default_var="datatel_dw")
BQ_LOCATION = Variable.get("datatel_bq_location", default_var="US")
BQ_CONNECTION = Variable.get(
    "datatel_bq_connection",
    default_var="projects/your-gcp-project/locations/US/connections/datatel_postgres",
)

WINDOW_PARAMETERS = {
    "window_start": "{{ params.window_start if params.window_start else data_interval_start.isoformat() }}",
    "window_end": "{{ params.window_end if params.window_end else data_interval_end.isoformat() }}",
    "lookback_start": "{{ params.lookback_start if params.lookback_start else (data_interval_start - macros.timedelta(days=params.lookback_days)).isoformat() }}",
}

BIGQUERY_PARAMS = {
    "bq_project": BQ_PROJECT,
    "bq_dataset": BQ_DATASET,
    "bq_connection": BQ_CONNECTION,
}

DW_COLUMNS = [
    "customer_id",
    "customer_name",
    "email",
    "country",
    "customer_since",
    "total_revenue",
    "total_transactions",
    "total_data_used_mb",
    "avg_session_duration_sec",
    "total_sessions",
    "arpu",
    "short_sessions",
    "medium_sessions",
    "long_sessions",
    "avg_data_per_session_mb",
]

DW_SCHEMA = [
    bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("customer_name", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("customer_since", "TIMESTAMP"),
    bigquery.SchemaField("total_revenue", "NUMERIC"),
    bigquery.SchemaField("total_transactions", "INTEGER"),
    bigquery.SchemaField("total_data_used_mb", "NUMERIC"),
    bigquery.SchemaField("avg_session_duration_sec", "NUMERIC"),
    bigquery.SchemaField("total_sessions", "INTEGER"),
    bigquery.SchemaField("arpu", "NUMERIC"),
    bigquery.SchemaField("short_sessions", "INTEGER"),
    bigquery.SchemaField("medium_sessions", "INTEGER"),
    bigquery.SchemaField("long_sessions", "INTEGER"),
    bigquery.SchemaField("avg_data_per_session_mb", "NUMERIC"),
    bigquery.SchemaField("refreshed_at", "TIMESTAMP"),
]


def serialize_for_bigquery(value):
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def load_dw_user_analytics_to_bigquery() -> None:
    postgres = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    client = bigquery.Client(project=BQ_PROJECT)
    dataset_ref = bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}")
    dataset_ref.location = BQ_LOCATION
    client.create_dataset(dataset_ref, exists_ok=True)

    source_sql = """
        SELECT
            c.customer_id,
            c.customer_name,
            c.email,
            c.country,
            c.customer_since,
            coalesce(r.total_revenue, 0) AS total_revenue,
            coalesce(r.total_transactions, 0) AS total_transactions,
            coalesce(u.total_data_used_mb, 0) AS total_data_used_mb,
            coalesce(u.avg_session_duration_sec, 0) AS avg_session_duration_sec,
            coalesce(u.total_sessions, 0) AS total_sessions,
            coalesce(a.arpu, 0) AS arpu,
            coalesce(d.short_sessions, 0) AS short_sessions,
            coalesce(d.medium_sessions, 0) AS medium_sessions,
            coalesce(d.long_sessions, 0) AS long_sessions,
            coalesce((u.total_data_used_mb / nullif(u.total_sessions, 0))::numeric(18, 2), 0) AS avg_data_per_session_mb
        FROM stg_customers c
        LEFT JOIN agg_user_revenue r ON r.customer_id = c.customer_id
        LEFT JOIN agg_user_usage u ON u.customer_id = c.customer_id
        LEFT JOIN agg_arpu a ON a.customer_id = c.customer_id
        LEFT JOIN agg_session_distribution d ON d.customer_id = c.customer_id
    """

    rows = []
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(source_sql)
            for result in cursor.fetchall():
                row = {
                    column: serialize_for_bigquery(value)
                    for column, value in zip(DW_COLUMNS, result)
                }
                row["refreshed_at"] = pendulum.now("UTC").isoformat()
                rows.append(row)

    final_table = f"{BQ_PROJECT}.{BQ_DATASET}.dw_user_analytics"

    load_job = client.load_table_from_json(
        rows,
        final_table,
        job_config=bigquery.LoadJobConfig(
            schema=DW_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    load_job.result()


with DAG(
    dag_id="datatel_daily_pipeline_generated",
    description="Daily SQL-first telecom warehouse pipeline for DataTel Communications.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    template_searchpath=[str(PROJECT_DIR)],
    params={
        "window_start": "",
        "window_end": "",
        "lookback_start": "",
        "lookback_days": 2,
    },
    tags=["datatel", "warehouse", "sql-first"],
) as dag:
    start = EmptyOperator(task_id="start")

    create_postgres_schema = SQLExecuteQueryOperator(
        task_id="create_postgres_schema",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "00_schema" / "00_postgres_schema.sql"),
    )

    check_null_primary_identifiers = SQLExecuteQueryOperator(
        task_id="check_null_primary_identifiers",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "01_quality" / "01_null_primary_identifiers.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    check_duplicate_transaction_ids = SQLExecuteQueryOperator(
        task_id="check_duplicate_transaction_ids",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "01_quality" / "02_duplicate_transaction_ids.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    check_duplicate_session_ids = SQLExecuteQueryOperator(
        task_id="check_duplicate_session_ids",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "01_quality" / "03_duplicate_session_ids.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    check_invalid_session_times = SQLExecuteQueryOperator(
        task_id="check_invalid_session_times",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "01_quality" / "04_invalid_session_times.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    load_stg_billing = SQLExecuteQueryOperator(
        task_id="load_stg_billing",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "02_staging" / "01_stg_billing.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    load_stg_sessions = SQLExecuteQueryOperator(
        task_id="load_stg_sessions",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "02_staging" / "02_stg_sessions.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    load_stg_customers = SQLExecuteQueryOperator(
        task_id="load_stg_customers",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "02_staging" / "03_stg_customers.sql"),
    )

    agg_user_revenue = SQLExecuteQueryOperator(
        task_id="agg_user_revenue",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "03_transform" / "01_agg_user_revenue.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    agg_user_usage = SQLExecuteQueryOperator(
        task_id="agg_user_usage",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "03_transform" / "02_agg_user_usage.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    agg_monthly_revenue = SQLExecuteQueryOperator(
        task_id="agg_monthly_revenue",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "03_transform" / "03_agg_monthly_revenue.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    agg_arpu = SQLExecuteQueryOperator(
        task_id="agg_arpu",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "03_transform" / "04_agg_arpu.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    session_buckets = SQLExecuteQueryOperator(
        task_id="session_buckets",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "03_transform" / "05_session_buckets.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    agg_session_distribution = SQLExecuteQueryOperator(
        task_id="agg_session_distribution",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "03_transform" / "06_agg_session_distribution.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    load_bigquery_table = PythonOperator(
        task_id="load_bigquery_table",
        python_callable=load_dw_user_analytics_to_bigquery,
    )

    update_watermark = SQLExecuteQueryOperator(
        task_id="update_watermark",
        conn_id=POSTGRES_CONN_ID,
        sql=read_sql(POSTGRES_SQL_DIR / "03_transform" / "07_update_watermark.sql"),
        parameters=WINDOW_PARAMETERS,
    )

    finish = EmptyOperator(task_id="finish")

    start >> create_postgres_schema

    create_postgres_schema >> [
        check_null_primary_identifiers,
        check_duplicate_transaction_ids,
        check_duplicate_session_ids,
        check_invalid_session_times,
    ]

    [check_null_primary_identifiers, check_duplicate_transaction_ids] >> load_stg_billing
    [check_null_primary_identifiers, check_duplicate_session_ids, check_invalid_session_times] >> load_stg_sessions
    check_null_primary_identifiers >> load_stg_customers

    load_stg_billing >> [agg_user_revenue, agg_monthly_revenue]
    agg_monthly_revenue >> agg_arpu

    load_stg_sessions >> [agg_user_usage, session_buckets]
    session_buckets >> agg_session_distribution

    [
        load_stg_customers,
        agg_user_revenue,
        agg_user_usage,
        agg_arpu,
        agg_session_distribution,
    ] >> load_bigquery_table >> update_watermark >> finish
