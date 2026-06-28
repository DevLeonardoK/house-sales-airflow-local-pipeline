from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import IntegerType, StringType, DoubleType, LongType, BooleanType, TimestampType
from minio import Minio
import os
from dotenv import load_dotenv

load_dotenv()

bronze_minio_path = "s3a://bronze/kc_house_data.csv"

silver_minio_path = "s3a://silver/silver.parquet"

def build_spark():

        try:

                spark = SparkSession.builder.appName("silver_app")\
                        .master("spark://spark-master:7077")\
                        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
                        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_USER"))\
                        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_PASS"))\
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

                df_preco_padroes = df_preco_padroes.where(
                        (condicao) & (F.col("price").cast(DoubleType())> 0)
                )

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


def fix_floors(df_fix_id):

        try:

                df_fix_floors = df_fix_id

                df_fix_floors = df_fix_floors.withColumn("floors",
                        F.when(
                                F.col("floors").rlike(r"[^\d.,]"), F.regexp_replace(F.col("floors"), r"[^\d.,]", "" )
                        ).otherwise(F.col("floors"))
                )

                df_fix_floors = df_fix_floors.withColumn(
                        "floors", 
                        F.regexp_replace(
                                F.col("floors"), ",", "."
                        )
                )

                df_fix_floors = df_fix_floors.withColumn(
                        "floors", F.when(
                                        (F.col("floors").cast(DoubleType()) % 1 != 0) & (F.col("sqft_basement").cast(DoubleType()) <= 0),
                                        F.floor(F.col("floors").cast(DoubleType()))
                                ).otherwise(F.col("floors").cast(DoubleType()))
                )

                df_fix_floors = df_fix_floors.where(F.col("floors").cast(DoubleType()) >= 0)

                return df_fix_floors

        except Exception:
                raise

def fix_areas(df_fix_floors):

        try:

                df_fix_areas = df_fix_floors

                df_fix_areas = df_fix_areas.where(
                        (F.col("sqft_living").cast(DoubleType()) > 0) & (F.col("sqft_lot").cast(DoubleType()) > 0)
                )
        
                return df_fix_areas

        except Exception:
                raise

def apply_cast(df_fix_areas):

        try:

                df = df_fix_areas

                df = (
                        df
                        .withColumn("id", F.col("id").cast(LongType()))
                        .withColumn("date", F.to_timestamp("date", "yyyyMMdd'T'HHmmss").cast(TimestampType()))
                        .withColumn("price", F.col("price").cast(DoubleType()))
                        .withColumn("bathrooms", F.floor(F.col("bathrooms")).cast(IntegerType()))
                        .withColumn("bedrooms", F.floor(F.col("bedrooms")).cast(IntegerType()))
                        .withColumn("sqft_living", F.col("sqft_living").cast(DoubleType()))
                        .withColumn("sqft_lot", F.col("sqft_lot").cast(DoubleType()))
                        .withColumn("floors", F.col("floors").cast(IntegerType()))
                        .withColumn("waterfront", F.col("waterfront").cast(BooleanType()))
                        .withColumn("view", F.floor(F.col("view")).cast(IntegerType()))
                        .withColumn("condition", F.col("condition").cast(DoubleType()))
                        .withColumn("grade", F.col("grade").cast(DoubleType()))
                        .withColumn("sqft_above", F.col("sqft_above").cast(DoubleType()))
                        .withColumn("sqft_basement", F.col("sqft_basement").cast(DoubleType()))
                        .withColumn("yr_built", F.col("yr_built").cast(IntegerType()))
                        .withColumn("date", F.to_timestamp("date", "yyyy"))
                        .withColumn("yr_renovated", F.col("yr_renovated").cast(IntegerType()))
                        .withColumn("zipcode", F.col("zipcode").cast(StringType()))
                        .withColumn("lat", F.col("lat").cast(DoubleType()))
                        .withColumn("long", F.col("long").cast(DoubleType()))
                        .withColumn("sqft_living15", F.col("sqft_living15").cast(DoubleType()))
                        .withColumn("sqft_lot15", F.col("sqft_lot15").cast(DoubleType()))
                )
        
                return df
        
        except Exception:
                raise


def fix_yr_built_renovated(df):

        try:

                df = df

                df = df.where(
                        (
                         (F.col("yr_built") <= F.to_timestamp("date", "yyyy").cast(IntegerType())) &
                         (F.col("yr_renovated") >= F.col("yr_built")) & 
                         (F.col("yr_built") > 0)
                        )
                )

                return df
        
        except Exception:
                raise



def add_flags(df):

        try:
                df = df.withColumn("idade_imovel", F.year(F.current_date()) - F.col("yr_built").cast(IntegerType()))
                df = df.withColumn("foi_renovado", F.when(F.col("yr_renovated") > 0, True).otherwise(False))
                df = df.withColumn("tem_porao", F.when(F.col("sqft_basement") > 0, True).otherwise(False))
                df = df.withColumn("classificacao_tamanho_imovel", F.when(F.col("sqft_lot") <= 5000, "pequeno").when((F.col("sqft_lot") > 5000 ) & (F.col("sqft_lot") <= 10000), "medio").otherwise("grande"))

                return df

        except Exception:
                raise


def send_to_minio(df):

        try:

                client_minio = Minio(
                        endpoint="minio:9000",
                        access_key=os.getenv("MINIO_USER"),
                        secret_key=os.getenv("MINIO_PASS"),
                        secure=False
                )

                path = "/opt/airflow/files"

                minio_files_path = "s3a://silver/"

                os.makedirs(path, exist_ok=True)

                if client_minio.bucket_exists("silver") == False:
                        client_minio.make_bucket("silver")

                #Salvar dataset parquet
                df.write.mode("overwrite").parquet(f"{minio_files_path}/silver_original")
                df.write.mode("overwrite").partitionBy("idade_imovel").parquet(f"{minio_files_path}/part_idade_imovel")
                df.write.mode("overwrite").partitionBy("foi_renovado", "yr_renovated").parquet(f"{minio_files_path}/part_renovado_ano")
                df.write.mode("overwrite").partitionBy("classificacao_tamanho_imovel").parquet(f"{minio_files_path}/part_tamanho_imovel")

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
                
                fix_floors_v = fix_floors(fix_id_v)
                print("fix_floors - ok")

                fix_areas_v = fix_areas(fix_floors_v)
                print("fix_areas - ok")

                apply_cast_v = apply_cast(fix_areas_v)
                print("apply_cast - ok")

                fix_yr_built_renovated_v = fix_yr_built_renovated(apply_cast_v)
                print("fix_yr_built_renovated - ok")

                add_flags_v = add_flags(fix_yr_built_renovated_v)
                print("add_flags ok")
                add_flags_v.show(10)

                send_to_minio(add_flags_v)


        except Exception:
                raise
        finally:
                if fix_null_none_spaces_v.is_cached:
                        fix_null_none_spaces_v.unpersist()
                spark.stop()


if __name__ == "__main__":
        main()
