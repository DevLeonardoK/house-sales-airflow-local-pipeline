from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("silver").master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .getOrCreate()

SparkSession.getActiveSession()

df = spark.read.csv("s3a://bronze/kc_house_data.csv")
print(df.count())
