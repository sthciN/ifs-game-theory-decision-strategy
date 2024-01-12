from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def push_xcom(key, value, **context):
    context['ti'].xcom_push(key=key, value=value)

def pull_xcom(key, **context):
    value = context['ti'].xcom_pull(key=key)
    return value

class BigqieryData():
    def __init__(self, connection_id, ti, bigquery_dataset_id, bigquery_table_name):
        self.connection_id = connection_id
        self.ti = ti
        self.bigquery_dataset_id = bigquery_dataset_id
        self.bigquery_table_name = bigquery_table_name
        self.bq = BigQueryHook(bigquery_conn_id=self.connection_id,
                        use_legacy_sql=False)
    
    def get_tabledata(self, xcom_key):
        result = self.bq.get_tabledata(dataset_id=self.bigquery_dataset_id, 
                     table_id=self.bigquery_table_name,
                     )
        self.ti.xcom_push(key=xcom_key, value=result)
        
        return True
