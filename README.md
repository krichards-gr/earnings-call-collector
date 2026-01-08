# Earnings Call Collector

A hybrid tool to retrieve, process, and normalize earnings call transcripts using DuckDB and the DefeatBeta API. It supports both local execution (saving to SQLite/CSV) and cloud deployment via Google Cloud Run (saving to BigQuery).

## Overview

This project fetches earnings call transcripts for specified stock tickers, normalizes the data into a relational structure (Metadata + Content), and stores it.

*   **Local Mode**: Runs in a WSL environment. Saves data to local `transcripts.db` (SQLite) and CSV files (`transcripts_metadata.csv`, `transcripts_content.csv`). Syncs new data to BigQuery if credentials are set up.
*   **Cloud Mode**: Runs as a Google Cloud Run Function. Triggered via HTTP. Saves data *only* to BigQuery (ephemeral storage is skipped).

## Project Structure

*   **`sql_get.py`**: Core script. Fetches and processes transcripts.
*   **`main.py`**: Entry point for Google Cloud Run Functions.
*   **`db_cloud_utils.py`**: Utilities for BigQuery interaction (schema setup, insertion).
*   **`setup_bq.py`**: Script to initialize the BigQuery dataset and tables one-time.
*   **`run_in_wsl.ps1`**: PowerShell wrapper for local execution via WSL.
*   **`setup_and_run.sh`**: Bash script for environment setup and execution in WSL.
*   **`tickers.csv`**: List of ticker symbols to query.

## Cloud Deployment (Google Cloud Run)

The project is designed to be deployed as a 2nd Gen Cloud Function (Cloud Run).

### 1. Prerequisites
*   Google Cloud Project with BigQuery and Cloud Run APIs enabled.
*   DeepLake/HuggingFace API access (via `defeatbeta-api`).

### 2. Configuration
The function uses the following constants (configure in `sql_get.py` or `db_cloud_utils.py`):
*   `PROJECT_ID`: Your GCP Project ID.
*   `DATASET_ID`: BigQuery dataset name (e.g., `pressure_monitoring`).

### 3. BigQuery Schema
The system uses two tables in BigQuery:
1.  **`earnings_call_transcript_metadata`**:
    *   `transcript_id` (STRING, REQ): Unique MD5 hash of `symbol` + `report_date`.
    *   `symbol`, `report_date`, `fiscal_year`, `fiscal_quarter`.
2.  **`earnings_call_transcript_content`**:
    *   `transcript_id` (STRING, REQ).
    *   `paragraph_number`, `speaker`, `content`.

### 4. Triggering the Function
The function expects an HTTP request. You can pass optional parameters in the JSON body or Query String:
*   `tickers`: (Optional) Ticker symbol or source.
*   `months`: (Optional) Number of months back to search (int). Default is 1.

## Local Development & Usage

For local runs, the project prefers a Linux environment (or WSL on Windows).

### 1. Configure Tickers
Add symbols to `tickers.csv`:
```csv
symbol
AAPL
MSFT
```

### 2. Run the Collector
Use the PowerShell wrapper to execute inside WSL:

**Standard Run (YTD):**
```powershell
.\run_in_wsl.ps1
```

**Custom Date Range:**
```powershell
.\run_in_wsl.ps1 -months 6
```

### 3. Local Output
*   **`transcripts_metadata.csv`**: Call metadata.
*   **`transcripts_content.csv`**: Full transcript text.

## ID Generation
`transcript_id` is a deterministic MD5 hash of `symbol` + `report_date`, ensuring:
*   Uniform IDs across local and cloud environments.
*   Idempotency (re-running does not create duplicate IDs).
