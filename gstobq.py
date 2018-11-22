from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    'C:\\Users\\talih\Desktop\\BigQuerytoTableau-6d00b31bb9ab.json')
project_id = 'bigquery-to-tableau'

client = bigquery.Client(credentials= credentials,project=project_id)
table_ref = client.dataset('BigTableau').table('dataflowbasics_schemas')


job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY 
job_config.skip_leading_rows = 1
job_config.autodetect = True
job_config.allow_jagged_rows = True
job_config.ignore_unknown_values = True
job_config.max_bad_records = 1000
schema = [
        bigquery.SchemaField('merchant_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('merchant', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('chain_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('agent_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('city_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('district_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('maincategory_name_en', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('partner_manager_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('event_type', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('event_time', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('dinner_time', 'TIMESTAMP', mode='NULLABLE'),
        bigquery.SchemaField('reservation_type', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('agent', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('marketing_channel', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('reservation_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('status', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('seated_guests', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('cost', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('impressions', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('clicks', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('pageviews', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('unique_pageviews', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('transaction_gross_margin_eur', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('transaction_gross_margin_local', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('subscription_gross_margin_euro', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('subscription_gross_margin_local', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('rating', 'INTEGER', mode='NULLABLE'),
    ]
job_config.schema = schema
uri = 'gs://desctinations3tostorage/merchant20180829.csv'
load_job = client.load_table_from_uri(
    uri,
    table_ref,
    job_config=job_config)  # API request

assert load_job.job_type == 'load'

load_job.result()  # Waits for table load to complete.

assert load_job.state == 'DONE'
