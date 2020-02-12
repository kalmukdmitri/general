import pandas as pd
import os
from google.cloud import bigquery
from google.oauth2 import service_account
class gbq_pd:
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="CarWeGo-Analytics-b48469538956.json"
    client = bigquery.Client()
    credential = service_account.Credentials.from_service_account_file('CarWeGo-Analytics-b48469538956.json',)

    def __init__(self, table_name, datasetId = 'icapbi' ):
        self.client = gbq_pd.client
        self.table_name = table_name
        self.datasetId = datasetId
        self.data = gbq_pd.client.get_table(f'{datasetId}.{table_name}').to_api_repr()
        self.columns = [i['name'] for i in self.data['schema']['fields']]
        self.schema = self.data['schema']['fields']
        self.credentials = gbq_pd.credential
        self.projectId = self.data['tableReference']['projectId']
        
    def add(self, values, if_exists = 'fail'):
        insertable_df  = pd.DataFrame(values, columns = self.columns)

        insertable_df.to_gbq(f'{self.datasetId}.{self.table_name}', project_id=self.projectId,
                             if_exists=if_exists, credentials=self.credentials ,table_schema = self.schema)
        

    def clear_table(self):
        
        fld = [i['name'] for i in self.data['schema']['fields'] if i['type']  == 'STRING'][0]
        client = bigquery.Client()
        q =(f"""
        DELETE 
        FROM {self.datasetId}.{self.table_name}
        WHERE {fld} != 'TETETETET'
            """)
        query_job = self.client.query(q)
        results = query_job.result()
        return results
    
    
    def df_query(self, q):
        import pandas_gbq
        results = pandas_gbq.read_gbq(q, project_id=self.projectId, credentials=self.credentials)
        return results
    