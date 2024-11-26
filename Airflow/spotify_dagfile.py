from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
import kagglehub
import os

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'spotify_data_to_adls',
    default_args=default_args,
    description='Download Spotify data from Kaggle and upload to Azure ADLS',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 11),
    catchup=False,
) as dag:

    def download_spotify_data():
        # Download dataset from Kaggle
        path = kagglehub.dataset_download("dhruvildave/spotify-charts")
        print("Path to dataset files:", path)
        return path

    def upload_to_azure_adls(local_path):
        # Retrieve environment variables
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

        # Connect to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # Upload each file in the downloaded dataset directory
        for root, dirs, files in os.walk(local_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                blob_client = container_client.get_blob_client(file_name)
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                print(f"Uploaded {file_name} to Azure ADLS.")

    # Tasks
    download_task = PythonOperator(
        task_id='download_spotify_data',
        python_callable=download_spotify_data
    )

    upload_task = PythonOperator(
        task_id='upload_to_azure_adls',
        python_callable=upload_to_azure_adls,
        op_kwargs={'local_path': '{{ ti.xcom_pull(task_ids="download_spotify_data") }}'},
    )

    # Set task dependencies
    download_task >> upload_task
