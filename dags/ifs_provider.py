import os
from datetime import datetime

from dotenv import find_dotenv, load_dotenv
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpdateTableSchemaOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from utils.helpers import BigqieryData


ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)


project_id = os.getenv("PROJECT_ID")
dataset_path = os.getenv("DATASET_PATH")
dataset_csv_name = os.getenv("DATASET_CSV_NAME")
bucket_name = os.getenv("BUCKET_NAME")
connection_id = os.getenv("CONNECTION_ID")
bigquery_dataset_id = os.getenv("BIGQUERY_DATASET")
bigquery_table_name = os.getenv("BIGQUERY_TABLE_NAME")

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ifs"],
)
def ifs_dag():
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

    # [START schema_field_update]
    schema_field_update = BigQueryUpdateTableSchemaOperator(
        task_id="schema_field_update",
        schema_fields_updates=[{"name": "Status", "type": "STRING"}],
        dataset_id=bigquery_dataset_id,
        table_id=bigquery_table_name,
        include_policy_tags="False",
        project_id=project_id,
        gcp_conn_id=connection_id,
    )
    # [END schema_field_update]

    # [START raw_data_check]
    @task
    def raw_data_check(scan_name='raw_data_check', check_path='raw'):
        from include.soda.checks import check

        return check(scan_name, checks_subpath=check_path)
    
    raw_data_check = raw_data_check()
    # [END raw_data_check]

    transform_indicator = DbtTaskGroup(
        group_id='transform_indicator',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform_indicator']
        )
    )

    # [START transform_data_check]
    @task
    def transform_data_check(scan_name='transform_data_check', check_path='transform'):
        from include.soda.checks import check

        return check(scan_name, checks_subpath=check_path)
    
    transform_data_check = transform_data_check()
    # [END transform_data_check]


    @task
    def nash():
        from utils.nash import nash_equiblirium
        nash_equiblirium(connection_id=connection_id, bigquery_dataset_id=bigquery_dataset_id, project_id=project_id)
        
        return True

    @task
    def country_indicator_data(ti):
        ti.xcom_push(key='connection_id', value=connection_id)
        bqd = BigqieryData(connection_id=connection_id,
                        ti=ti,
                        bigquery_dataset_id=bigquery_dataset_id,
                        bigquery_table_name="indicator",
                        ).get_tabledata()
        
        return bqd
    
    @task
    def country_data(ti): 
        bqd = BigqieryData(connection_id=connection_id,
                        ti=ti,
                        bigquery_dataset_id=bigquery_dataset_id,
                        bigquery_table_name="country",
                        ).get_tabledata()
        
        return bqd

    @task
    def create_country_strategy_table():
        from utils.strategy import decision_strategies
        bigquerydata = BigqieryData(project_id=project_id,
                                    connection_id=connection_id,
                                    bigquery_dataset_id=bigquery_dataset_id
                                    )
        schema_fields = [
            {"name": "country", "type": "STRING", "mode": "REQUIRED"},
       ]
        for key, _ in decision_strategies.items():
            schema_fields.append({"name": key, "type":"STRING", "mode":"REPEATED"})

        bigquerydata.create_table(table_id='strategy_country', schema=schema_fields)
        
        return True


    @task
    def create_strategy_dataframe():
        import json
        from utils.strategy_extractor import get_strategy_dataframe
        bigquerydata = BigqieryData(project_id=project_id,
                                    connection_id=connection_id,
                                    bigquery_dataset_id=bigquery_dataset_id
                                    )

        countries = bigquerydata.get_tabledata(bigquery_table_name="country")
        indicators = bigquerydata.get_tabledata(bigquery_table_name="indicator")
        country_strategy = get_strategy_dataframe(countries, indicators)
        bigquerydata.insert_into_bigquery(rows=country_strategy, table_id='strategy_country')
        
        return True

    transform_strategy = DbtTaskGroup(
        group_id='transform_strategy',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform_strategy']
        )
    )
    local_csv_to_gcs
    chain([local_csv_to_gcs, create_dataset] >> 
     gcs_to_bq >> 
     schema_field_update >> 
     raw_data_check >> 
     transform_indicator >> 
     transform_data_check >>
     create_country_strategy_table() >>
     create_strategy_dataframe() >>
     transform_strategy >>
     nash()
    )

ifs_dag()
