from minio import Minio
import kagglehub as kaggle
import os

dataset_kaggle_path = "harlfoxem/housesalesprediction"

def send_to_minio():
    try:
        client_minio = Minio(
            endpoint = "minio:9000",
            access_key = "minio",
            secret_key = "minio123",
            secure = False
        )
    except Exception as e:
        raise Exception(f"Failed to connect to MinIO: {e}")

    try:
        bucket_name = "bronze"
        if not client_minio.bucket_exists(bucket_name):
            client_minio.make_bucket(bucket_name)

        dataset_path = kaggle.dataset_download(dataset_kaggle_path)

        csv_file = os.path.join(dataset_path, "kc_house_data.csv")

        client_minio.fput_object(bucket_name, "kc_house_data.csv", csv_file)

    except Exception as e:
        raise Exception(f"An error occurred: {e}")