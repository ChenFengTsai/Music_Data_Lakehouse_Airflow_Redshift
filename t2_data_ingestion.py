import os
from datetime import timedelta
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.sql import SQLValueCheckOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

S3_BUCKET = Variable.get("data_lake_bucket")
SCHEMA = "spotify_table"

data_count = {
    'song': 200,
    'artist': 200,
    'album': 200,
}

BEGIN_DATE = "2022-10-01"
END_DATE = "2023-01-31"

DEFAULT_ARGS = {
    "owner": "chenfeng",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    # Here the two database are the same
    "redshift_conn_id": "spotify_database",
    "postgres_conn_id": "spotify_database",
}

# set start_date as days_ago(1) if wanna test it
with DAG(
    dag_id=DAG_ID,
    description="Copy and merge of data into Redshift",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=datetime(2023, 3, 1),
    schedule_interval=None,
    tags=["spotify table"],
) as dag:

    for table in data_count.keys():
        create_staging_tables = PostgresOperator(
            task_id=f"create_{table}_staging",
            sql=f"sql/create_staging/create_{table}_staging.sql",
        )
        # if table exists then truncate the past data
        truncate_staging_tables = PostgresOperator(
            task_id=f"truncate_{table}_staging",
            sql=f"TRUNCATE TABLE {SCHEMA}.{table}_staging;",
        )

        s3_to_staging_tables = S3ToRedshiftOperator(
            task_id=f"{table}_to_staging",
            s3_bucket=S3_BUCKET,
            s3_key=f"redshift/data/{table}.csv",
            schema=SCHEMA,
            table=f"{table}_staging",
            copy_options=['DELIMITER ',' CSV', 'IGNOREHEADER 1'],
        )

        merge_staging_data = PostgresOperator(
            task_id=f"merge_{table}",
            sql=f"sql/merge_staging/merge_{table}.sql",
            params={"begin_date": BEGIN_DATE, "end_date": END_DATE},
        )

        drop_staging_tables = PostgresOperator(
            task_id=f"drop_{table}_staging",
            sql=f"DROP TABLE IF EXISTS {SCHEMA}.{table}_staging;",
        )

        check_data_counts = SQLValueCheckOperator(
            task_id=f"check_data_count_{table}",
            conn_id=DEFAULT_ARGS["redshift_conn_id"],
            sql=f"SELECT COUNT(*) FROM {SCHEMA}.{table}",
            pass_value=data_count[table],
        )
        
        (create_staging_tables>>truncate_staging_tables>>s3_to_staging_tables
         >>merge_staging_data>>drop_staging_tables>>check_data_counts)