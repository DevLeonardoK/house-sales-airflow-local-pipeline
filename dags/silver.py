from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG
import datetime

with DAG(
    dag_id="silver",
    start_date=datetime.datetime(2026,6,24),
    schedule="@daily"
) as dag:
    silver_medallion = SparkSubmitOperator(
        task_id = "silver_medallion",
        application="/opt/airflow/jobs/silver.py",
        conn_id="spark_master"
    )