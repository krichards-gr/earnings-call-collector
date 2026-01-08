from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ID = "sri-benchmarking-databases"
DATASET_ID = "pressure_monitoring"

def fix_duplicates():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    
    metadata_table = f"{dataset_ref}.earnings_call_transcript_metadata"
    content_table = f"{dataset_ref}.earnings_call_transcript_content"

    # 1. Deduplicate Metadata
    # Strategy: Keep one row per transcript_id
    logger.info(f"Deduplicating {metadata_table}...")
    query_meta = f"""
    CREATE OR REPLACE TABLE `{metadata_table}` AS
    SELECT * EXCEPT(rn)
    FROM (
      SELECT *, ROW_NUMBER() OVER(PARTITION BY transcript_id ORDER BY report_date) as rn
      FROM `{metadata_table}`
    )
    WHERE rn = 1
    """
    try:
        job = client.query(query_meta)
        job.result()
        logger.info("Metadata table deduplicated.")
    except Exception as e:
        logger.error(f"Error deduplicating metadata: {e}")

    # 2. Deduplicate Content
    # Strategy: Keep one row per transcript_id + paragraph_number
    # Note: If there are multiple identical rows, any is fine.
    logger.info(f"Deduplicating {content_table}...")
    query_content = f"""
    CREATE OR REPLACE TABLE `{content_table}` AS
    SELECT * EXCEPT(rn)
    FROM (
      SELECT *, ROW_NUMBER() OVER(PARTITION BY transcript_id, paragraph_number ORDER BY transcript_id) as rn
      FROM `{content_table}`
    )
    WHERE rn = 1
    """
    try:
        job = client.query(query_content)
        job.result()
        logger.info("Content table deduplicated.")
    except Exception as e:
        logger.error(f"Error deduplicating content: {e}")

if __name__ == "__main__":
    fix_duplicates()
