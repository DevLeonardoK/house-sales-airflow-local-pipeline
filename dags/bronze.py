#directed acyclic graph
import sys
sys.path.insert(0, '/opt/airflow')

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import datetime
from jobs.bronze import send_to_minio

with DAG(
    dag_id = "bronze",
    start_date = datetime.datetime(2026,6,13),
    schedule = "@daily" 
) as dag:
    ingest_bronze = PythonOperator(
        task_id = "ingest_bronze",
        python_callable=send_to_minio
    )