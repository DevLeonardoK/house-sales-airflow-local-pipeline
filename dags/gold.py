from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

with DAG(
    dag_id="gold",
    start_date=datetime.datetime(2026, 6, 24),
    schedule=None,
    catchup=False,
) as dag:

    ddl_database = SQLExecuteQueryOperator(
        task_id="ddl_database",
        conn_id="postgres_default",
        sql="sql/create_table_gold.sql",
    )

    gold_batch = SparkSubmitOperator(
        task_id="gold",
        conn_id=os.getenv("SPARK_MASTER_CONN_ID"),
        application="/opt/airflow/jobs/gold.py",
    )

    ddl_database >> gold_batch
