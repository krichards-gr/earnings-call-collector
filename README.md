# Earnings Call Collector

A set of tools to retrieve, process, and normalize earnings call transcripts using DuckDB and the DefeatBeta API.

## Overview

This project fetches earnings call transcripts for specified stock tickers, normalizes the data into a relational structure (Metadata + Content), and saves the results as CSV files. It is designed to run in a WSL (Windows Subsystem for Linux) environment to ensure compatibility with specific dependencies.

## Project Structure

*   **`sql_get.py`**: The core Python script that performs the SQL query via DuckDB, fetches transcripts, and normalizes the data.
*   **`run_in_wsl.ps1`**: PowerShell wrapper script to easily execute the tool inside WSL from Windows.
*   **`setup_and_run.sh`**: Bash script that handles environment setup (pip installation) and runs the Python script within WSL.
*   **`tickers.csv`**: Input CSV file containing the list of ticker symbols to query (header: `symbol`).

## Prerequisites

*   **WSL (Windows Subsystem for Linux)**: Required for running the scripts.
*   **Python 3**: Must be installed in your WSL environment.
*   **Dependencies**: The script automatically checks for and installs required Python packages (`pandas`, `duckdb`, `defeatbeta-api`).

## Usage

### 1. Configure Tickers
Add the stock symbols you want to track to `tickers.csv`. The file should have a header row:
```csv
symbol
AAPL
MSFT
TSLA
```

### 2. Run the Collector
You can run the collector using the PowerShell wrapper. By default, it fetches Year-to-Date (YTD) data.

**Standard Run (YTD):**
```powershell
.\run_in_wsl.ps1
```

**Custom Date Range (Last N months):**
To fetch data for the last 6 months:
```powershell
.\run_in_wsl.ps1 -months 6
```

## Output

The tool generates two normalized CSV files linked by a unique `transcript_id`.

### 1. `transcripts_metadata.csv`
Contains high-level information about each earnings call.
*   **transcript_id**: Unique 32-character MD5 hash of `symbol` + `report_date`. (Primary Key)
*   **symbol**: Stock ticker.
*   **report_date**: Date of the report.
*   **fiscal_year**: Fiscal year of the report.
*   **fiscal_quarter**: Fiscal quarter of the report.

### 2. `transcripts_content.csv`
Contains the actual text content of the call, broken down by paragraph.
*   **transcript_id**: Foreign Key linking to the metadata file.
*   **paragraph_number**: Sequence number of the paragraph.
*   **speaker**: Name of the speaker (if available).
*   **content**: The spoken text.

## ID Generation
The `transcript_id` is generated using a deterministic MD5 hash of the `symbol` and `report_date`. This ensures that:
*   IDs are uniform across all records.
*   IDs are reproducible (running the script twice on the same data yields the same IDs).
*   Data integrity is maintained even if source data has missing native IDs.
