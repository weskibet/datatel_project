from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from datatel.config import (
    PG_CONN_ID,
    BQ_CONN_ID,
    BQ_PROJECT,
    BQ_STAGING_DS,
    BQ_WAREHOUSE_DS,
    LOOKBACK_DAYS,
)

# -------------------------
# Paths
# -------------------------
SQL_DIR = Path(__file__).parent / "sql"


def sql_file(path: str) -> str:
    return str(SQL_DIR / path)


# -------------------------
# Default args (production tuned)
# -------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# -------------------------
# Quality Gate
# -------------------------
def quality_gate(source_prefix: str, **context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    run_date = context["ds"]

    rows = hook.get_records(
        """
        SELECT check_name, issue_count
        FROM validation_results
        WHERE check_date = %s
          AND source_table LIKE %s
          AND status = 'FAIL'
        """,
        parameters=(run_date, f"{source_prefix}%"),
    )

    if rows:
        failures = ", ".join([f"{r[0]} ({r[1]})" for r in rows])
        raise ValueError(f"DATA QUALITY FAILED: {failures}")

    return True


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="datatel_daily_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["datatel", "etl", "production"],
    params={
        "lookback_days": LOOKBACK_DAYS,
        "bq_project": BQ_PROJECT,
        "bq_staging_dataset": BQ_STAGING_DS,
        "bq_warehouse_dataset": BQ_WAREHOUSE_DS,
    },
    template_searchpath=[str(SQL_DIR.parent)],
) as dag:

    # -------------------------
    # Setup
    # -------------------------
    setup = PostgresOperator(
        task_id="setup_control_tables",
        postgres_conn_id=PG_CONN_ID,
        sql=sql_file("setup/control_tables.sql"),
    )

    # -------------------------
    # Data Quality Checks
    # -------------------------
    quality_checks = [
        PostgresOperator(
            task_id="check_null_billing",
            postgres_conn_id=PG_CONN_ID,
            sql=sql_file("quality/check_null_ids_billing.sql"),
        ),
        PostgresOperator(
            task_id="check_null_sessions",
            postgres_conn_id=PG_CONN_ID,
            sql=sql_file("quality/check_null_ids_sessions.sql"),
        ),
        PostgresOperator(
            task_id="check_duplicates_billing",
            postgres_conn_id=PG_CONN_ID,
            sql=sql_file("quality/check_duplicate_transactions.sql"),
        ),
        PostgresOperator(
            task_id="check_duplicates_sessions",
            postgres_conn_id=PG_CONN_ID,
            sql=sql_file("quality/check_duplicate_sessions.sql"),
        ),
    ]

    quarantine = PostgresOperator(
        task_id="quarantine_bad_records",
        postgres_conn_id=PG_CONN_ID,
        sql=sql_file("quality/quarantine_records.sql"),
    )

    gate_billing = PythonOperator(
        task_id="gate_billing",
        python_callable=quality_gate,
        op_kwargs={"source_prefix": "src_billing"},
    )

    gate_sessions = PythonOperator(
        task_id="gate_sessions",
        python_callable=quality_gate,
        op_kwargs={"source_prefix": "src_network_sessions"},
    )

    # -------------------------
    # Staging Layer
    # -------------------------
    stg_billing = PostgresOperator(
        task_id="stg_billing",
        postgres_conn_id=PG_CONN_ID,
        sql=sql_file("staging/stg_billing.sql"),
    )

    stg_sessions = PostgresOperator(
        task_id="stg_sessions",
        postgres_conn_id=PG_CONN_ID,
        sql=sql_file("staging/stg_sessions.sql"),
    )

    stg_customers = PostgresOperator(
        task_id="stg_customers",
        postgres_conn_id=PG_CONN_ID,
        sql=sql_file("staging/stg_customers.sql"),
    )

    # -------------------------
    # BigQuery Transformations
    # -------------------------
    def bq_task(task_id, file):
        return BigQueryExecuteQueryOperator(
            task_id=task_id,
            gcp_conn_id=BQ_CONN_ID,
            sql=sql_file(file),
            use_legacy_sql=False,
        )

    agg_user_revenue = bq_task("agg_user_revenue", "transform/agg_user_revenue.sql")
    agg_user_usage = bq_task("agg_user_usage", "transform/agg_user_usage.sql")
    agg_monthly_revenue = bq_task("agg_monthly_revenue", "transform/agg_monthly_revenue.sql")
    agg_arpu = bq_task("agg_arpu", "transform/agg_arpu.sql")
    session_buckets = bq_task("session_buckets", "transform/session_buckets.sql")
    agg_session_distribution = bq_task(
        "agg_session_distribution",
        "transform/agg_session_distribution.sql",
    )

    dw_user_analytics = bq_task(
        "dw_user_analytics",
        "warehouse/dw_user_analytics.sql",
    )

    # -------------------------
    # DAG Flow
    # -------------------------

    setup >> quality_checks >> quarantine

    setup >> stg_customers

    quarantine >> gate_billing
    quality_checks >> gate_sessions

    gate_billing >> stg_billing
    gate_sessions >> stg_sessions

    stg_billing >> agg_user_revenue
    stg_sessions >> agg_user_usage
    stg_customers >> agg_monthly_revenue

    [agg_user_revenue, agg_user_usage, agg_monthly_revenue] >> agg_arpu

    agg_arpu >> session_buckets >> agg_session_distribution

    [
        agg_user_revenue,
        agg_user_usage,
        agg_monthly_revenue,
        agg_arpu,
        agg_session_distribution,
    ] >> dw_user_analytics