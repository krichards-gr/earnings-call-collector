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
        print(f"Found {len(result)} transcripts.")
        
        # Reorder columns: symbol, report_date first
        cols = ['symbol', 'report_date'] + [c for c in result.columns if c not in ['symbol', 'report_date']]
        result = result[cols]

        # Save to CSV
        output_file = 'transcript_data_sql.csv'
        result.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")

except Exception as e:
    print(f"Error: {e}")