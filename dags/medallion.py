from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from jobs.bronze import send_to_minio
import datetime

with DAG(
    dag_id = "medallion",
    start_date = datetime.datetime(2026, 6, 24),
    schedule= None
) as dag:
    
    ingest_bronze = PythonOperator(
        task_id = "bronze",
        python_callable = send_to_minio
    )

    silver_prepare = SparkSubmitOperator(
        task_id = "silver",
        conn_id = "spark_master",
        application = "/opt/airflow/jobs/silver.py"
    )

    ingest_bronze >> silver_prepare