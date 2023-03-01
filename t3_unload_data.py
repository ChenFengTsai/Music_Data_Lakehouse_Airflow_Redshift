import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

S3_BUCKET = Variable.get("data_lake_bucket")
S3_UNLOAD_PATH = f"s3://{S3_BUCKET}/redshift/song_detail/"
REDSHIFT_UNLOAD_IAM_ROLE = Variable.get("redshift_unload_iam_role")

DEFAULT_ARGS = {
    "owner": "chenfeng",
    "depends_on_past": False,
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "postgres_conn_id": "spotify_database",
}

dag = DAG(
    dag_id=DAG_ID,
    description="Upload data from Redshift to S3",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=datetime(2023, 3, 1),
    schedule_interval=None,
    tags=["spotify table"],)

unload_data = PostgresOperator(
    task_id="unload_data",
    sql="sql/unload.sql",
    params={
        "s3_unload_path": S3_UNLOAD_PATH,
        "redshift_unload_iam_role": REDSHIFT_UNLOAD_IAM_ROLE,
    },
    dag=dag
)


