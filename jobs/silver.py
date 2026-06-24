from pyspark.sql import SparkSession

bronze_minio_path = "s3a://bronze/kc_house_data.csv"

def build_spark():
        try:
                spark = SparkSession.builder.appName("silver_app")\
                        .master("spark://spark-master:7077")\
                        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
                        .config("spark.hadoop.fs.s3a.access.key", "minio")\
                        .config("spark.hadoop.fs.s3a.secret.key", "minio123")\
                        .config("spark.hadoop.fs.s3a.path.style.access", "true")\
                        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
                        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
                        .config("spark.hadoop.fs.s3a.fast.upload", "true")\
                        .getOrCreate()
                return spark
        except Exception:
                raise

def read_csv_raw(spark):
        try:
                data_raw = spark.read.csv(bronze_minio_path, header = True, inferSchema = True)
                data_raw.show()
                return data_raw
        except Exception:
                raise

def main():
        spark = build_spark()
        try:
                read_csv_raw(spark)
        except Exception:
                raise
        finally:
                spark.stop()

if __name__ == "__main__":
        main()
