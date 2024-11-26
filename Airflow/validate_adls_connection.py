from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
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
    'validate_adls_connection',
    default_args=default_args,
    description='DAG to validate Azure Data Lake Storage connection',
    schedule_interval=None,
    start_date=datetime(2024, 11, 11),
    catchup=False,
) as dag:

    def validate_adls():
        # Retrieve connection string from environment variables
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        
        # Check if the environment variables are set
        if not connection_string or not container_name:
            raise ValueError("Azure connection string or container name not found in environment variables.")

        # Connect to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # List blobs in the specified container
        print(f"Listing blobs in container '{container_name}':")
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            print(f"- {blob.name}")

    # Task to validate ADLS connection
    validate_adls_task = PythonOperator(
        task_id='validate_adls_connection',
        python_callable=validate_adls
    )
