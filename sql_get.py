import defeatbeta_api
import logging
import pandas as pd
from defeatbeta_api.client.duckdb_client import DuckDBClient
from defeatbeta_api.client.duckdb_client import Configuration
from defeatbeta_api.client.hugging_face_client import HuggingFaceClient
from defeatbeta_api.utils.const import stock_earning_call_transcripts

import argparse
import datetime
import ast
import hashlib
import os
import db_utils
import db_cloud_utils

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants for BQ
PROJECT_ID = "sri-benchmarking-databases"
DATASET_ID = "pressure_monitoring"

def collect_transcripts(tickers_source, months=None):
    """
    Main logic to collect transcripts.
    tickers_source: Path to CSV or list of tickers.
    months: Number of months back to retrieve.
    """
    
    # Initialize clients
    duckdb_client = DuckDBClient(log_level=logging.INFO, config=Configuration(threads=8))
    huggingface_client = HuggingFaceClient()

    # Initialize DBs
    db_utils.initialize_db()
    existing_ids_local = db_utils.get_existing_ids()
    
    # Try to get BQ existing IDs
    try:
        existing_ids_bq = db_cloud_utils.get_existing_ids_bq(PROJECT_ID, DATASET_ID)
        logger.info(f"Loaded {len(existing_ids_bq)} existing transcript IDs from BigQuery.")
    except Exception as e:
        logger.warning(f"Could not load BQ IDs: {e}")
        existing_ids_bq = set()

    logger.info(f"Loaded {len(existing_ids_local)} existing transcript IDs from local database.")

    # Date logic
    if months:
        cutoff_date = (datetime.date.today() - datetime.timedelta(days=months*30)).strftime('%Y-%m-%d')
        logger.info(f"Retrieving data from last {months} months (since {cutoff_date})")
    else:
        cutoff_date = '2024-01-01'
        logger.info(f"Retrieving data since {cutoff_date}")

    # Read tickers
    tickers = []
    if isinstance(tickers_source, list):
        tickers = tickers_source
    elif isinstance(tickers_source, str):
        try:
            tickers_df = pd.read_csv(tickers_source)
            if 'symbol' in tickers_df.columns:
                tickers = tickers_df['symbol'].tolist()
            elif 'ticker' in tickers_df.columns:
                tickers = tickers_df['ticker'].tolist()
            else:
                tickers = tickers_df.iloc[:, 0].tolist()
        except Exception as e:
            logger.error(f"Error reading tickers file: {e}")
            return
            
    if not tickers:
        logger.warning("No tickers found to process.")
        return

    tickers_str = ", ".join([f"'{t}'" for t in tickers])
    logger.info(f"Querying for {len(tickers)} tickers.")

    # Get data URL
    url = huggingface_client.get_url_path(stock_earning_call_transcripts)

    # Construct SQL query
    sql = f"SELECT * FROM '{url}' WHERE symbol IN ({tickers_str}) AND CAST(report_date AS DATE) >= '{cutoff_date}'"

    logger.info("Executing DuckDB query...")
    result = duckdb_client.query(sql)

    if result.empty:
        logger.info("No transcripts found for the specified criteria.")
        return

    logger.info(f"Retrieved {len(result)} potential matches. Processing for new calls...")
    
    metadata_rows = []
    content_rows = []
    new_calls_count = 0

    for index, row in result.iterrows():
        try:
            # 1. Identity and Deduplication
            id_str = f"{row['symbol']}{row['report_date']}"
            transcript_id = hashlib.md5(id_str.encode()).hexdigest()
            
            # User Request: BQ is the main source.
            if transcript_id in existing_ids_bq:
                continue
            
            # Note: We do NOT skip even if it is in existing_ids_local, 
            # because we might need to backfill BQ. 
            
            new_calls_count += 1
            
            # 2. Extract Metadata
            metadata_rows.append({
                'transcript_id': transcript_id,
                'symbol': row['symbol'],
                'report_date': row['report_date'],
                'fiscal_year': row['fiscal_year'],
                'fiscal_quarter': row['fiscal_quarter']
            })
            
            # BQ Doc Init
            bq_doc = {
                'transcript_id': transcript_id,
                'symbol': row['symbol'],
                'report_date': str(row['report_date']), # Ensure string for BQ DATE
                'fiscal_year': row['fiscal_year'],
                'fiscal_quarter': row['fiscal_quarter'],
                'content': []
            }

            # 3. Extract Content
            transcript_raw = row['transcripts']
            
            if isinstance(transcript_raw, str):
                paragraphs = ast.literal_eval(transcript_raw)
            elif isinstance(transcript_raw, list):
                paragraphs = transcript_raw
            elif hasattr(transcript_raw, 'tolist'):
                paragraphs = transcript_raw.tolist()
            else:
                logger.warning(f"Unexpected type for transcripts: {row['symbol']} {row['report_date']} {type(transcript_raw)}")
                continue
            
            for p in paragraphs:
                p_num = p.get('paragraph_number')
                speaker = p.get('speaker')
                content_text = p.get('content')
                
                content_rows.append({
                    'transcript_id': transcript_id,
                    'paragraph_number': p_num,
                    'speaker': speaker,
                    'content': content_text
                })

        except (ValueError, SyntaxError) as e:
            logger.error(f"Error parsing transcript for {row['symbol']} on {row['report_date']}: {e}")
            continue

    if new_calls_count == 0:
        logger.info("No new calls found for local DB.")
        return

    # SAVE TO LOCAL
    # We must only write to local if it doesn't exist locally (to avoid UNIQUE constraint fail)
    # even though we processed it for the sake of BQ.
    
    # Check if running in Cloud Run (K_SERVICE is set automatically)
    is_cloud_run = os.environ.get('K_SERVICE') is not None
    
    # Filter for local
    local_metadata_rows = [r for r in metadata_rows if r['transcript_id'] not in existing_ids_local]
    local_content_rows = [r for r in content_rows if r['transcript_id'] not in existing_ids_local]

    if not is_cloud_run and local_metadata_rows:
        logger.info(f"Saving {len(local_metadata_rows)} new calls to SQLite and CSV...")
        metadata_df = pd.DataFrame(local_metadata_rows).drop_duplicates()
        content_df = pd.DataFrame(local_content_rows)

        db_utils.insert_metadata(metadata_df)
        db_utils.insert_content(content_df)
        
        # Save to CSVs (Append mode)
        metadata_file = 'transcripts_metadata.csv'
        content_file = 'transcripts_content.csv'
        
        if os.path.exists(metadata_file):
            metadata_df.to_csv(metadata_file, mode='a', header=False, index=False)
        else:
            metadata_df.to_csv(metadata_file, index=False)
            
        if os.path.exists(content_file):
            content_df.to_csv(content_file, mode='a', header=False, index=False)
        else:
            content_df.to_csv(content_file, index=False)
    elif is_cloud_run:
        logger.info("Running in Cloud Run: Skipping local DB/CSV writes (ephemeral storage).")
    else:
        logger.info("All processed calls already exist in local DB (skipping local write).")

    # SAVE TO BQ
    if metadata_rows: 
        logger.info(f"Saving {len(metadata_rows)} new calls to BigQuery...")
        try:
             # Since we controlled the loop using existing_ids_bq, everything in metadata_rows
             # is strictly NEW for BigQuery. We can write it all.
             
             bq_metadata_df = pd.DataFrame(metadata_rows).drop_duplicates()
             bq_content_df = pd.DataFrame(content_rows)
             
             if not bq_metadata_df.empty:
                 db_cloud_utils.insert_metadata_bq(PROJECT_ID, DATASET_ID, bq_metadata_df)
                 db_cloud_utils.insert_content_bq(PROJECT_ID, DATASET_ID, bq_content_df)
                 logger.info(f"Saved {len(bq_metadata_df)} calls to BigQuery.")

        except Exception as e:
            logger.error(f"Failed to save to BigQuery: {e}")


if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser(description='Retrieve earning call transcripts.')
    parser.add_argument('--tickers', type=str, default='tickers.csv', help='Path to CSV file containing tickers')
    parser.add_argument('--months', type=int, help='Number of months back to retrieve data for')
    args = parser.parse_args()

    collect_transcripts(args.tickers, args.months)
