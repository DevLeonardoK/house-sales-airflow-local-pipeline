from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from functools import reduce

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

def fix_null_none_spaces(spark):

        try:

                df = read_csv_raw(spark)
                df_columns = df.columns

                for c in df_columns:
                        df = df.withColumn(c, F.regexp_replace(c, r"\s+", ""))

                df = df.dropna()

                df_sem_espacos = df.cache()

                return df_sem_espacos
        
        except Exception:
                raise

def fix_price(fix_null_none_spaces):

        try:

                df_preco_padroes = fix_null_none_spaces

                #troca virgula por ponto
                df_preco_padroes = df_preco_padroes.withColumn(
                        "price",
                        F.when(F.col("price").rlike(r"^\d+(\,\d+)?$"), F.regexp_replace("price", ",","."))
                        .otherwise(F.col("price"))
                )


                colunas_numericas_selecionadas = [c for c in df_preco_padroes.columns if c not in ["id", "date", "waterfront", "view", "price", "long"]]

                condicao = reduce(
                        lambda x,y: x & y,
                        [F.col(c).rlike(r"^\d+([.]\d+)?$").alias(c) for c in colunas_numericas_selecionadas]
                )

                df_preco_padroes = df_preco_padroes.where(condicao)

                return df_preco_padroes
        
        except Exception:
                raise


def fix_date(df_precos_padroes):
        
        try:

                df_fix_datas = df_precos_padroes

                condicao = F.col("date").rlike(r"^\d{8}T\d{6}$") # 99999999T999999

                df_fix_datas = df_fix_datas.where(condicao)

                return df_fix_datas
        
        except Exception:
                raise

def fix_waterfront(df_fix_datas):

        try:

                df_fix_waterfront = df_fix_datas

                df_fix_waterfront = df_fix_waterfront.withColumn(
                        "waterfront", F.when(F.col("waterfront") > 0, True).otherwise(False)
                )

                return df_fix_waterfront
        
        except Exception:
                raise


def fix_view(df_fix_waterfront):

        try:

                df_fix_view = df_fix_waterfront

                condicao = F.col("view") >=0

                df_fix_view = df_fix_view.where(condicao)

                return df_fix_view

        except Exception:
                raise

def fix_long(df_fix_view):

        try:

                df_fix_long = df_fix_view

                df_fix_long = df_fix_long.withColumn(
                "long",
                        F.when(
                                F.col("long").rlike(r"^\d+(,\d+)?$"),
                                F.regexp_replace(F.col("long"), ",", ".")
                        ).otherwise(F.col("long"))
                )

                df_fix_long = df_fix_long.dropna()
                         
                condicao = F.col("long").rlike(r"^-?\d+([.]\d+)?$").alias("long_rlike")

                df_fix_long = df_fix_long.where(condicao == True)

                return df_fix_long

        except Exception:
                raise


def fix_id(fix_long):

        try:

                df_fix_id = fix_long
                
                df_fix_id = df_fix_id.withColumn(
                        "id", F.when(
                                        F.col("id").rlike(r"\D"),
                                        F.regexp_replace(F.col("id"), r"\D", "")
                                ).otherwise(F.col("id"))
                )

                condicao = F.col("id") >= 0

                df_fix_id = df_fix_id.where(condicao)

                return df_fix_id
        
        except Exception:
                raise


def main():
        spark = build_spark()
        try:
                read_csv_raw(spark)
                print("read_csv_raw - ok")

                fix_null_none_spaces_v = fix_null_none_spaces(spark)
                print("remover_espacos - ok")

                fix_price_v = fix_price(fix_null_none_spaces_v)
                print("fix_price - ok")

                fix_date_v = fix_date(fix_price_v)
                print("fix_date - ok")

                fix_waterfront_v = fix_waterfront(fix_date_v)
                print("fix_waterfront_v - ok")

                fix_view_v = fix_view(fix_waterfront_v)
                print("fix_views - ok")

                fix_long_v = fix_long(fix_view_v)
                print("fix_long - ok")
                
                fix_id_v = fix_id(fix_long_v)
                print("fix_id - ok")
                fix_id_v.show()

        except Exception:
                raise
        finally:
                if fix_null_none_spaces_v.is_cached:
                        fix_null_none_spaces_v.unpersist()
                spark.stop()


if __name__ == "__main__":
        main()
