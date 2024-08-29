from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

class BigqieryData():
    def __init__(self, project_id, connection_id, bigquery_dataset_id):
        self.project_id = project_id
        self.connection_id = connection_id
        self.bigquery_dataset_id = bigquery_dataset_id
        self.bq = BigQueryHook(bigquery_conn_id=self.connection_id,
                        use_legacy_sql=False)
    
    def get_tabledata(self, bigquery_table_name):
        result = self.bq.get_tabledata(dataset_id=self.bigquery_dataset_id, 
                     table_id=bigquery_table_name,
                     )
        
        return result

    def insert_into_bigquery(self, rows, table_id):
        self.bq.insert_all(project_id=self.project_id,
                           dataset_id=self.bigquery_dataset_id,
                           table_id=table_id,
                           rows=rows
                           )
        
        return True

    def create_table(self, table_id, schema):
        self.bq.create_empty_table(project_id=self.project_id,
                                   dataset_id=self.bigquery_dataset_id,
                                   table_id=table_id,
                                   schema_fields=schema
                                   )
        
        return True
