from airflow.models import DagBag
from astro import sql as aql
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator


def test_ifs_dag_loaded():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("ifs")

    assert dag is not None, "DAG 'ifs' not found"

def test_ifs_dag_structure():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("ifs")

    tasks = dag.tasks
    assert len(tasks) == 3, "DAG 'ifs' should have 3 tasks"

def test_ifs_dag_task_types():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("ifs")

    tasks = dag.tasks
    local_csv_to_gcs = tasks[0]
    create_dataset = tasks[1]
    gcs_to_bq = tasks[2]

    assert isinstance(local_csv_to_gcs, LocalFilesystemToGCSOperator), "Task 'local_csv_to_gcs' should be an instance of LocalFilesystemToGCSOperator"
    assert isinstance(create_dataset, BigQueryCreateEmptyDatasetOperator), "Task 'create_dataset' should be an instance of BigQueryCreateEmptyDatasetOperator"
    assert isinstance(gcs_to_bq, aql.load_file), "Task 'gcs_to_bq' should be an instance of aql.load_file"

def test_ifs_dag_task_ids():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("ifs")

    tasks = dag.tasks
    local_csv_to_gcs = tasks[0]

    assert local_csv_to_gcs.task_id == "local_csv_to_gcs", "Task 'local_csv_to_gcs' should have task_id 'local_csv_to_gcs'"