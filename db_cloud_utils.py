from google.cloud import bigquery
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_client(project_id):
    return bigquery.Client(project=project_id)

def initialize_bq(project_id, dataset_id):
    client = get_client(project_id)
    dataset_ref = f"{project_id}.{dataset_id}"
    
    # Metadata Table
    metadata_table_id = f"{dataset_ref}.transcript_metadata"
    metadata_schema = [
        bigquery.SchemaField("transcript_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("report_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("fiscal_year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("fiscal_quarter", "INTEGER", mode="NULLABLE"),
    ]
    _create_table_if_not_exists(client, metadata_table_id, metadata_schema)

    # Content Table
    content_table_id = f"{dataset_ref}.transcript_content"
    content_schema = [
        bigquery.SchemaField("transcript_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("paragraph_number", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("speaker", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("content", "STRING", mode="NULLABLE"),
    ]
    _create_table_if_not_exists(client, content_table_id, content_schema)

def _create_table_if_not_exists(client, table_ref, schema):
    try:
        table = bigquery.Table(table_ref, schema=schema)
        try:
            client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists.")
        except Exception:
            client.create_table(table)
            logger.info(f"Created table {table_ref}")
    except Exception as e:
        logger.error(f"Error checking/creating table {table_ref}: {e}")
        raise

def get_existing_ids_bq(project_id, dataset_id):
    client = get_client(project_id)
    table_ref = f"{project_id}.{dataset_id}.transcript_metadata"
    
    query = f"SELECT transcript_id FROM `{table_ref}`"
    
    try:
        try:
            client.get_table(table_ref)
        except Exception:
            return set()
            
        query_job = client.query(query)
        results = query_job.result()
        return set(row.transcript_id for row in results)
    except Exception as e:
        logger.warning(f"Could not fetch existing IDs from BigQuery: {e}")
        return set()

def insert_metadata_bq(project_id, dataset_id, df):
    if df.empty:
        return
    client = get_client(project_id)
    table_ref = f"{project_id}.{dataset_id}.transcript_metadata"
    _insert_rows_from_df(client, table_ref, df)

def insert_content_bq(project_id, dataset_id, df):
    if df.empty:
        return
    client = get_client(project_id)
    table_ref = f"{project_id}.{dataset_id}.transcript_content"
    _insert_rows_from_df(client, table_ref, df)

def _insert_rows_from_df(client, table_ref, df):
    try:
        # data needs to be json-serializable (DATE objects -> str)
        # However, insert_rows_from_dataframe is easier if we have pandas
        # But for portability and explicit types we might want insert_rows_json
        # Let's try load_table_from_dataframe for better bulk performance/type handling
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result() # Wait for job to complete
        logger.info(f"Successfully loaded {len(df)} rows into {table_ref}.")
    except Exception as e:
        logger.error(f"Failed to load data into {table_ref}: {e}")
