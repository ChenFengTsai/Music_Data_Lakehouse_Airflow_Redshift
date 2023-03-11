import os
from datetime import timedelta
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

S3_BUCKET = Variable.get("data_lake_bucket")
S3_UNLOAD_PATH = f"s3://{S3_BUCKET}/redshift/song_detail/"
GLUE_CRAWLER_IAM_ROLE = Variable.get("glue_crawler_iam_role")
GLUE_DATABASE = "song_db"

Athena_result = Variable.get('athena_query_results')
Athena_output = f"s3://{Athena_result}/

query = """SELECT *
            FROM song_db.song
            WHERE catgroup = 'Shows' AND catname = 'Opera'
            ORDER BY saletime
            LIMIT 10;"""

DEFAULT_ARGS = {
    "owner": "chenfeng",
    "depends_on_past": False,
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "postgres_conn_id": "spotify_database",
}

with DAG(
    dag_id=DAG_ID,
    description="Catalog unloaded data with Glue and query with Athena",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=datetime(2023, 3, 1),
    schedule_interval=None,
    tags=["spotify table"],
) as dag:

    delete_glue_catalog = BashOperator(
        task_id="delete_catalog",
        bash_command=f'aws glue delete-database --name {GLUE_DATABASE} || echo "Database {GLUE_DATABASE} not found."',
    )

    create_glue_catalog = BashOperator(
        task_id="create_catalog",
        bash_command="""aws glue create-database --database-input \
            '{"Name": "song_db", "Description": "data unloaded from Redshift"}'""",
    )

    crawl_unloaded_data = AwsGlueCrawlerOperator(
        task_id="crawl_unloaded_data",
        config={
            "Name": "song-details-unloaded",
            "Role": GLUE_CRAWLER_IAM_ROLE,
            "DatabaseName": GLUE_DATABASE,
            "Description": "Crawl song data unloaded from Redshift",
            "Targets": {"S3Targets": [{"Path": S3_UNLOAD_PATH}]},
        },
    )
    
    # list out the table names and output as a tabular format (not necessary but good check)
    list_glue_tables = BashOperator(
        task_id="list_glue_tables",
        bash_command=f"""aws glue get-tables --database-name {GLUE_DATABASE} \
                          --query 'TableList[].Name' \
                          --output table""",
    )

    athena_query_glue = AWSAthenaOperator(
        task_id="athena_query_glue",
        query=query,
        output_location= Athena_output,
        database=GLUE_DATABASE,
    )

    (delete_glue_catalog >> create_glue_catalog >> crawl_unloaded_data >> list_glue_tables >> athena_query_glue)
