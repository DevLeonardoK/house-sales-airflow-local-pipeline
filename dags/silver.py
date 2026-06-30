from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

with DAG(
    dag_id="silver",
    start_date=datetime.datetime(2026, 6, 24),
    schedule=None,
    catchup=False,
) as dag:
    silver_medallion = SparkSubmitOperator(
        task_id="silver_medallion",
        application="/opt/airflow/jobs/silver.py",
        conn_id=os.getenv("SPARK_MASTER_CONN_ID"),
    )
