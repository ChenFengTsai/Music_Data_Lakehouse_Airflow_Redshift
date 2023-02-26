import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

S3_BUCKET = Variable.get("data_lake_bucket")
SCHEMA = "spotify_table"

### fix here and the sql file #####
TABLES = ['song', 'artist', 'album']


DEFAULT_ARGS = {
    "owner": "chenfeng",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "postgres_conn_id": "spotify_database",
}

# set start_date as days_ago(1) if wanna test it
dag = DAG(
    dag_id = DAG_ID,
    description = "Create spotify table schema tables in Redshift",
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(minutes=15),
    start_date = datetime(2023, 3, 1),
    schedule_interval = None,
    tags = ["spotify table"],
)


create_tables = PostgresOperator(
    task_id="create_tables", sql="sql/create_tables.sql",
    dag = dag
)

