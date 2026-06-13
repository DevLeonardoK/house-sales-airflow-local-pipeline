from minio import Minio
import kagglehub as kaggle
import os

path_to_save = "/home/devleonardo/data_home_lab/house-sales-airflow-local-pipeline/data/"

dataset_kaggle_path = "harlfoxem/housesalesprediction"

def download_csv():
    try:
        kaggle.dataset_download(dataset_kaggle_path, output_dir=path_to_save)
    except Exception as e:
        raise Exception(f"Failed to download dataset: {e}")

def send_to_minio():
    try:
        client_minio = Minio(
            endpoint = "minio:9000",
            access_key = "minio",
            secret_key = "minio123"
        )
    except Exception as e:
        raise Exception(f"Failed to connect: {e}")
    
    try:

        bucket_name = "bronze"
        if client_minio.bucket_exists(bucket_name):
            pass
        else:
            client_minio.make_bucket(bucket_name)
        
        download_csv()

        csv_name_bucket = "housesalesprediction.csv"

        client_minio.fput_object(bucket_name, csv_name_bucket, path_to_save)
    
    except Exception as e:
        raise Exception(f"An error occurred while uploading the CSV file to the MinIO bucket: {e}")