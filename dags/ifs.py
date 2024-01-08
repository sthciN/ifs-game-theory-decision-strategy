import os
from datetime import datetime

from dotenv import find_dotenv, load_dotenv
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.decorators import dag

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)


dataset_path = os.getenv("DATASET_PATH")
dataset_csv_name = os.getenv("DATASET_CSV_NAME")
bucket_name = os.getenv("BUCKET_NAME")
connection_id = os.getenv("CONNECTION_ID")
bigquery_dataset_id = os.getenv("BIGQUERY_DATASET")
bigquery_table_name = os.getenv("BIGQUERY_TABLE_NAME")

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ifs"],
)
def get_ifs_dts():
    # [START operator_local_to_gcs]
    local_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="local_csv_to_gcs",
        src=dataset_path,
        dst=dataset_csv_name,
        bucket=bucket_name,
        gcp_conn_id=connection_id,
        mime_type="text/csv",
    )
    # [END operator_local_to_gcs]


    # [START operator_create_empty_dataset]
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=bigquery_dataset_id,
        gcp_conn_id=connection_id,
    )
    # [END operator_create_empty_dataset]


    # [START operator_gcs_to_bigquery]
    gcs_to_bq = aql.load_file(
        task_id="gcs_to_bq",
        input_file=File(
            f"gs://{bucket_name}/{dataset_csv_name}",
            conn_id=connection_id,
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name=bigquery_table_name,
            conn_id=connection_id,
            metadata=Metadata(schema=bigquery_dataset_id)
        ),
        if_exists="replace",
    )
    # [END operator_gcs_to_bigquery]

    (local_csv_to_gcs >> create_dataset >> gcs_to_bq)

get_ifs_dts()
