import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator,BigQueryCreateEmptyDatasetOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "snappy-premise-456414-i7")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "my-bucket-snappy-premise-456414-i7")

dataset_file = "bitcoin-historical-data.zip"
dataset_csv = "btcusd_1-min_data.csv"
dataset_url = f"https://www.kaggle.com/api/v1/datasets/download/mczielinski/bitcoin-historical-data"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
path_to_creds = f"/.google/credentials/google_credentials.json"
parquet_file = dataset_csv.replace('.csv','.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'bitcoin_data')


def timestamp_cleanup(src_file):
    df = pd.read_csv(src_file)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'],unit='s')
    df['Time_date'] = df['Timestamp'].dt.date
    df['Time_time'] = df['Timestamp'].dt.time
    df.drop(columns='Timestamp', inplace=True)
    df.to_csv(src_file, index=False)


def format_to_parquet(src_file):
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
 "owner": "airflow",
 "start_date": days_ago(1),
 "depends_on_past": False,
 "retries": 1,
}

with DAG(
 dag_id="data_ingestion_gcp_dag",
 schedule_interval="@daily",
 default_args=default_args,
 catchup=False,
 max_active_runs=1,
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sS -L {dataset_url} -o {path_to_local_home}/{dataset_file}"
            )

    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip {path_to_local_home}/{dataset_file} -d {path_to_local_home}"
            )
    
    timestamp_cleanup_task = PythonOperator(
        task_id="timestamp_cleanup_task",
        python_callable=timestamp_cleanup,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_csv}"    
        },
        )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_csv}",
            },
            )

    upload_to_gcs_task = BashOperator(
        task_id="upload_to_gcs_task",
        bash_command=f"gcloud auth activate-service-account --key-file={path_to_creds} && \
                                            gsutil -m cp {path_to_local_home}/{parquet_file} gs://{BUCKET}"
            )

    bigquery_create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="bigquery_create_dataset_task",
        dataset_id="bitcoin_data",
        project_id="snappy-premise-456414-i7",
        exists_ok=True
        )


    bigquery_create_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_create_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "bitcoin_data",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/{parquet_file}"],
            },
            },
            )

    download_dataset_task >> unzip_dataset_task >> timestamp_cleanup_task >> format_to_parquet_task >> upload_to_gcs_task >> bigquery_create_dataset_task >> bigquery_create_table_task