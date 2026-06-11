import os
from airflow.models import Variable


def get_var(key: str, default=None, required: bool = False):
    """
    Safe config loader:
    Priority:
    1. Environment variable (Docker/K8s friendly)
    2. Airflow Variable
    3. Default value
    """
    value = os.getenv(key.upper())

    if value is None:
        value = Variable.get(key, default_var=default)

    if required and not value:
        raise ValueError(f"Missing required configuration: {key}")

    return value


# -------------------------
# Core Config
# -------------------------
PG_CONN_ID = get_var("datatel_pg_conn", default="postgres_default")
BQ_CONN_ID = get_var("datatel_bq_conn", default="google_cloud_default")

BQ_PROJECT = get_var("datatel_bq_project", required=True)

BQ_STAGING_DS = get_var("datatel_bq_staging_dataset", default="staging")
BQ_WAREHOUSE_DS = get_var("datatel_bq_warehouse_dataset", default="warehouse")

LOOKBACK_DAYS = int(get_var("datatel_lookback_days", default=3))