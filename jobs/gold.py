from pyspark.sql import SparkSession
from minio import Minio
import os
from dotenv import load_dotenv

load_dotenv()

silver_minio_path = os.getenv("SILVER_ORIGINAL_MINIO_PATH")

gold_minio_path = os.getenv("GOLD_MINIO_PATH")


def build_spark():
    spark = (
        SparkSession.builder.appName("gold_app")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_PASS"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )
    return spark


def get_silver_original_parquet(spark):
    df = spark.read.parquet(silver_minio_path)
    return df


def send_data_to_database(df):
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("truncate", "true")
        .option("url", os.getenv("PG_URL"))
        .option("dbtable", "gold.house_sales_gold")
        .option("user", os.getenv("PG_USER"))
        .option("password", os.getenv("PG_PASS"))
        .option("driver", "org.postgresql.Driver")
        .save()
    )


def send_gold_to_minio(df):
    client_minio = Minio(
        endpoint="minio:9000",
        access_key=os.getenv("MINIO_USER"),
        secret_key=os.getenv("MINIO_PASS"),
        secure=False,
    )

    if not client_minio.bucket_exists("gold"):
        client_minio.make_bucket("gold")

    df.write.mode("overwrite").parquet(f"{gold_minio_path}house_sales_gold_parquet")
    df.write.mode("overwrite").csv(f"{gold_minio_path}house_sales_gold_csv")


def main():
    spark = build_spark()
    try:
        silver_original = get_silver_original_parquet(spark)
        send_data_to_database(silver_original)
        send_gold_to_minio(silver_original)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
