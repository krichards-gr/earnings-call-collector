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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize clients
duckdb_client = DuckDBClient(log_level=logging.DEBUG, config=Configuration(threads=8))
huggingface_client = HuggingFaceClient()

# Argument parsing
parser = argparse.ArgumentParser(description='Retrieve earning call transcripts.')
parser.add_argument('--tickers', type=str, default='tickers.csv', help='Path to CSV file containing tickers')
parser.add_argument('--months', type=int, help='Number of months back to retrieve data for')
args = parser.parse_args()

# Initialize DB
db_utils.initialize_db()
existing_ids = db_utils.get_existing_ids()
logger.info(f"Loaded {len(existing_ids)} existing transcript IDs from database.")

# Date logic
if args.months:
    cutoff_date = (datetime.date.today() - datetime.timedelta(days=args.months*30)).strftime('%Y-%m-%d')
    logger.info(f"Retrieving data from last {args.months} months (since {cutoff_date})")
else:
    cutoff_date = '2024-01-01'
    logger.info(f"Retrieving data since {cutoff_date}")

# Read tickers
try:
    tickers_df = pd.read_csv(args.tickers)
    if 'symbol' in tickers_df.columns:
        tickers = tickers_df['symbol'].tolist()
    elif 'ticker' in tickers_df.columns:
        tickers = tickers_df['ticker'].tolist()
    else:
        tickers = tickers_df.iloc[:, 0].tolist()
    
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
    else:
        logger.info(f"Retrieved {len(result)} potential matches. Processing for new calls...")
        
        metadata_rows = []
        content_rows = []
        new_calls_count = 0

        for index, row in result.iterrows():
            try:
                # 1. Identity and Deduplication
                id_str = f"{row['symbol']}{row['report_date']}"
                transcript_id = hashlib.md5(id_str.encode()).hexdigest()
                
                if transcript_id in existing_ids:
                    continue
                
                new_calls_count += 1
                
                # 2. Extract Metadata
                metadata_rows.append({
                    'transcript_id': transcript_id,
                    'symbol': row['symbol'],
                    'report_date': row['report_date'],
                    'fiscal_year': row['fiscal_year'],
                    'fiscal_quarter': row['fiscal_quarter']
                })

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
                    content_rows.append({
                        'transcript_id': transcript_id,
                        'paragraph_number': p.get('paragraph_number'),
                        'speaker': p.get('speaker'),
                        'content': p.get('content')
                    })

            except (ValueError, SyntaxError) as e:
                logger.error(f"Error parsing transcript for {row['symbol']} on {row['report_date']}: {e}")
                continue

        if new_calls_count == 0:
            logger.info("No new calls found. Everything is up to date.")
        else:
            logger.info(f"Found {new_calls_count} new calls. Saving to database and CSVs...")
            
            # Create DataFrames
            metadata_df = pd.DataFrame(metadata_rows).drop_duplicates()
            content_df = pd.DataFrame(content_rows)

            # Save to SQLite
            db_utils.insert_metadata(metadata_df)
            db_utils.insert_content(content_df)
            logger.info(f"Saved {new_calls_count} calls to SQLite.")

            # Save to CSVs (Append mode)
            metadata_file = 'transcripts_metadata.csv'
            content_file = 'transcripts_content.csv'
            
            # Save Metadata CSV
            if os.path.exists(metadata_file):
                metadata_df.to_csv(metadata_file, mode='a', header=False, index=False)
            else:
                metadata_df.to_csv(metadata_file, index=False)
                
            # Save Content CSV
            if os.path.exists(content_file):
                content_df.to_csv(content_file, mode='a', header=False, index=False)
            else:
                content_df.to_csv(content_file, index=False)
            
            logger.info(f"Successfully appended {new_calls_count} calls to CSV logs.")

except Exception as e:
    logger.exception(f"An unexpected error occurred: {e}")
