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

def remover_espacos(spark):

        try:

                df = read_csv_raw(spark)
                df_columns = df.columns

                for c in df_columns:
                        df = df.withColumn(c, F.regexp_replace(c, r"\s+", ""))

                df = df.dropna()

                df_sem_espacos = df.cache()

                quantidade_string_vazia = df_sem_espacos.select(
                        [F.sum((F.col(c) == "").cast("int")).alias(c) for c in df_columns]
                )

                quantidade_null_none = df_sem_espacos.select(
                        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_columns]
                )

                quantidade_duplicadas = df_sem_espacos.groupBy(df_columns).count().filter(F.col("count") > 1)

                print("INFORMACOES ANALISE /ESPAÇO/VAZIO/NULL/NONE")
                print("-------------------------------------------")

                print("Strings vazias")
                quantidade_string_vazia.show()

                print("Null/None:")
                quantidade_null_none.show()

                print("Valores duplicados:")
                quantidade_duplicadas.show()

                return df_sem_espacos
        
        except Exception:
                raise

def fix_price(df_sem_espacos):

        try:

                df_preco_padroes = df_sem_espacos

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

                df_datas_certas = df_precos_padroes

                condicao = F.col("date").rlike(r"^\d{8}T\d{6}$") # 99999999T999999

                df_datas_certas = df_datas_certas.where(condicao)

                df_precos_padroes.unpersist()

                return df_datas_certas
        
        except Exception:
                raise

def main():
        spark = build_spark()
        try:
                read_csv_raw(spark)
                print("read_csv_raw - ok")
                remover_espacos_v = remover_espacos(spark)
                print("remover_espacos - ok")
                fix_price_v = fix_price(remover_espacos_v)
                print("fix_price - ok")
                fix_date_v = fix_date(fix_price_v)
                print("fix_date - ok")
        except Exception:
                raise
        finally:
                spark.stop()

if __name__ == "__main__":
        main()
