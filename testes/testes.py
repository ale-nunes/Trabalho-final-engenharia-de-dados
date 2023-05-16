from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import bigquery
import pandas as pd

# Autenticação
credentials = service_account.Credentials.from_service_account_file(
    'path/to/credentials.json',
    scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery'])

# Acesso aos dados do Google Drive
drive_service = build('drive', 'v3', credentials=credentials)
file_id = 'file_id'
file_content = drive_service.files().export(fileId=file_id, mimeType='text/csv').execute()

# Carregamento de dados no BigQuery
bq_client = bigquery.Client(credentials=credentials, project=project_id)
dataset_id = 'dataset_id'
table_id = 'table_id'
df = pd.read_csv(io.StringIO(file_content.decode('utf-8')))
table_ref = bq_client.dataset(dataset_id).table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = 'WRITE_TRUNCATE'
job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()
