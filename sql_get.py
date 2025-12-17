import defeatbeta_api
import logging
import pandas as pd
from defeatbeta_api.client.duckdb_client import DuckDBClient
from defeatbeta_api.client.duckdb_client import Configuration
from defeatbeta_api.client.hugging_face_client import HuggingFaceClient
from defeatbeta_api.utils.const import stock_earning_call_transcripts

import argparse
import datetime
# Initialize clients
duckdb_client = DuckDBClient(log_level=logging.DEBUG, config=Configuration(threads=8))
huggingface_client = HuggingFaceClient()

# Argument parsing
parser = argparse.ArgumentParser(description='Retrieve earning call transcripts.')
parser.add_argument('--tickers', type=str, default='tickers.csv', help='Path to CSV file containing tickers')
parser.add_argument('--months', type=int, help='Number of months back to retrieve data for')
args = parser.parse_args()

# Date logic
if args.months:
    # Explicit months back
    cutoff_date = (datetime.date.today() - datetime.timedelta(days=args.months*30)).strftime('%Y-%m-%d')
    print(f"Retrieving data from last {args.months} months (since {cutoff_date})")
else:
    # Default: Year to Date
    cutoff_date = datetime.date(datetime.date.today().year, 1, 1).strftime('%Y-%m-%d')
    print(f"Retrieving data YTD (since {cutoff_date})")

# Read tickers
try:
    tickers_df = pd.read_csv(args.tickers)
    # Assume first column is the ticker if 'symbol' or 'ticker' not present
    if 'symbol' in tickers_df.columns:
        tickers = tickers_df['symbol'].tolist()
    elif 'ticker' in tickers_df.columns:
        tickers = tickers_df['ticker'].tolist()
    else:
        tickers = tickers_df.iloc[:, 0].tolist()
    
    # Format for SQL IN clause
    tickers_str = ", ".join([f"'{t}'" for t in tickers])
    print(f"Querying for tickers: {tickers}")

    # Get data URL
    url = huggingface_client.get_url_path(stock_earning_call_transcripts)

    # Construct SQL query
    sql = f"SELECT * FROM '{url}' WHERE symbol IN ({tickers_str}) AND CAST(report_date AS DATE) >= '{cutoff_date}'"

    print(f"Executing query...")
    result = duckdb_client.query(sql)

    # Check if result is empty
    if result.empty:
        print("No transcripts found for the specified criteria.")
    else:
        print(f"Found {len(result)} transcripts. Flattening data...")
        
        flat_data = []
        import ast

        for index, row in result.iterrows():
            try:
                # Parse the transcripts string which is a list of dicts
                # Use ast.literal_eval because it's a string representation of a Python list
                transcript_raw = row['transcripts']
                
                if isinstance(transcript_raw, str):
                    paragraphs = ast.literal_eval(transcript_raw)
                elif isinstance(transcript_raw, list):
                    paragraphs = transcript_raw
                elif hasattr(transcript_raw, 'tolist'): # Check for numpy array
                    paragraphs = transcript_raw.tolist()
                else:
                    print(f"Unexpected type for transcripts: {row['symbol']} {row['report_date']} {type(transcript_raw)}")
                    continue
                
                for p in paragraphs:
                    flat_data.append({
                        'symbol': row['symbol'],
                        'report_date': row['report_date'],
                        'fiscal_year': row['fiscal_year'],
                        'fiscal_quarter': row['fiscal_quarter'],
                        'paragraph_number': p.get('paragraph_number'),
                        'speaker': p.get('speaker'),
                        'content': p.get('content'),
                        'transcript_id': row['transcripts_id']
                    })
            except (ValueError, SyntaxError) as e:
                print(f"Error parsing transcript for {row['symbol']} on {row['report_date']}: {e}")
                continue

        # Create new DataFrame
        flat_df = pd.DataFrame(flat_data)
        
        # Verify columns exist before reordering (in case data was empty)
        expected_cols = ['symbol', 'report_date', 'fiscal_year', 'fiscal_quarter', 'paragraph_number', 'speaker', 'content', 'transcript_id']
        if not flat_df.empty:
            flat_df = flat_df[expected_cols]

        # Save to CSV
        output_file = 'transcript_data_sql.csv'
        flat_df.to_csv(output_file, index=False)
        print(f"Flattened data saved to {output_file}. Total paragraphs: {len(flat_df)}")

except Exception as e:
    print(f"Error: {e}")