import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery


def get_pg_engine(pg_conn_id: str) -> str:
    """
    Build a SQLAlchemy connection string from Airflow connection metadata.
    In production, read from Airflow's Connection model.
    """
    # Placeholder — real implementation reads from
    # BaseHook.get_connection(pg_conn_id)
    return (
        f"postgresql+psycopg2://"
        f"{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
    )


def transfer_table(
    table_name: str,
    pg_conn_id: str,
    bq_project: str,
    bq_dataset: str,
):
    """Read a full table from PostgreSQL and overwrite it in BigQuery."""
    engine = create_engine(get_pg_engine(pg_conn_id))
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

    client = bigquery.Client(project=bq_project)
    table_id = f"{bq_project}.{bq_dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # block until complete
    print(f"Loaded {len(df)} rows into {table_id}")
