# Earnings Call Collector

A production-grade tool to retrieve, process, and normalize earnings call transcripts using the DefeatBeta API (powered by DuckDB queries). Supports both local execution and cloud deployment via Google Cloud Run with BigQuery storage.

## Overview

This tool programmatically fetches earnings call transcripts for specified stock tickers, normalizes the data into a relational structure (Metadata + Content), and stores it with intelligent deduplication.


### Architecture

**Data Flow:**
```
HuggingFace Parquet → Local Cache → DuckDB Query → Transcript Processing → BigQuery Storage
    (Source)           (1hr TTL)      (Filter)       (Deduplication)         (Output)
```

**Execution Modes:**
- **Cloud Mode** (Production): Runs as Google Cloud Run HTTP Function. Saves exclusively to BigQuery.
- **Local Mode** (Development): Runs via WSL. Saves to SQLite, CSV files, and optionally syncs to BigQuery.

**Key Features:**
- ✅ **BigQuery-first deduplication** - Uses BigQuery as authoritative source to prevent duplicates
- ✅ **Memory-optimized for Cloud Run** - Single-threaded DuckDB queries with 2GB memory allocation
- ✅ **Flexible date filtering** - Support for both `months` and `start_date` parameters
- ✅ **Idempotent operation** - Safe to re-run without creating duplicates
- ✅ **Parquet caching** - Downloads and caches HuggingFace parquet locally (1-hour TTL) to prevent 429 rate limiting
- ✅ **Automatic orphan healing** - Detects metadata rows missing content and backfills them on each run
- ✅ **Fortune 500 coverage** - Default ticker list covers full F500

## Project Structure

### Core Application
| File | Purpose |
|------|---------|
| **`sql_get.py`** | Core transcript collection logic with BigQuery deduplication and parquet caching |
| **`main.py`** | HTTP entry point for Google Cloud Run Functions |
| **`db_cloud_utils.py`** | BigQuery interaction utilities (schema, insertion, ID retrieval, orphan detection) |
| **`db_utils.py`** | Local SQLite database utilities (local mode only) |

### Configuration & Data
| File | Purpose |
|------|---------|
| **`tickers.csv`** | Full Fortune 500 ticker list (~500 symbols) |
| **`f500_tickers.csv`** | Fortune 500 ticker reference list |
| **`requirements.txt`** | Python dependencies |
| **`Dockerfile`** | Container image definition (Python 3.11-slim) |
| **`cloudbuild.yaml`** | Google Cloud Build pipeline (build, push, deploy to Cloud Run) |
| **`Procfile`** | Process definition for deployment |

### Scripts
| File | Purpose |
|------|---------|
| **`run_in_wsl.ps1`** | PowerShell wrapper for local WSL execution |
| **`run_backfill_in_wsl.ps1`** | PowerShell wrapper for content backfill |
| **`setup_and_run.sh`** | Bash script for environment setup in WSL |
| **`run.sh`** | Simple Python runner script |
| **`backfill.sh`** | Backfill content runner script |

### Utilities
| File | Purpose |
|------|---------|
| **`setup_bq.py`** | One-time BigQuery dataset and table initialization |
| **`backfill_content.py`** | Standalone backfill for orphaned metadata rows missing content |
| **`fix_duplicates.py`** | Emergency deduplication of BigQuery tables |
| **`discover_consts.py`** | DefeatBeta API constants discovery utility |

## Cloud Deployment (Google Cloud Run)

### Prerequisites
- Google Cloud Project with:
  - BigQuery API enabled
  - Cloud Run API enabled
  - Service account with BigQuery write permissions
- DefeatBeta API access (via `defeatbeta-api` package)

### Configuration

**Environment Detection:**
The code automatically detects Cloud Run via the `K_SERVICE` environment variable and applies optimizations:
- **Single-threaded DuckDB client** (reduces memory overhead)
- **Skips local DB operations** (no SQLite/CSV writes)
- **BigQuery-only storage** (ephemeral storage ignored)
- **Local parquet caching** (downloads HuggingFace parquet to `/tmp/parquet_cache` with 1-hour TTL to prevent 429 rate limiting)

**Required Constants** (in `sql_get.py`):
```python
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "pressure_monitoring"  # or your dataset name
```

### Memory Requirements

> **IMPORTANT**: Cloud Run service **must be configured with at least 2GB memory**.

The DuckDB query engine processes large datasets in memory. Default Cloud Run allocation (512MB) will cause crashes with SIGABRT errors.

**Set memory allocation:**
```bash
gcloud run services update earnings-call-collector \
  --memory 2Gi \
  --region us-central1
```

### BigQuery Schema

Two normalized tables are created in your dataset:

**1. `earnings_call_transcript_metadata`**
| Column | Type | Description |
|--------|------|-------------|
| `transcript_id` | STRING (REQ) | MD5 hash of `symbol` + `report_date` |
| `symbol` | STRING | Stock ticker symbol |
| `report_date` | DATE | Earnings call date |
| `fiscal_year` | INTEGER | Fiscal year |
| `fiscal_quarter` | INTEGER | Fiscal quarter (1-4) |

**2. `earnings_call_transcript_content`**
| Column | Type | Description |
|--------|------|-------------|
| `transcript_id` | STRING (REQ) | Foreign key to metadata |
| `paragraph_number` | INTEGER | Sequential paragraph number |
| `speaker` | STRING | Speaker name or role |
| `content` | STRING | Paragraph text content |

### HTTP API Parameters

The Cloud Function accepts parameters via **JSON body** or **URL query string**.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tickers` | string | `'tickers.csv'` | CSV file path or ticker list |
| `months` | integer | `1` | Number of months back to retrieve |
| `start_date` | string | `None` | Specific start date (YYYY-MM-DD format) |

**Parameter Priority:**
1. If `start_date` is provided → uses exact date (overrides `months`)
2. Else if `months` is provided → calculates as `today - (months × 30 days)`
3. Else → defaults to `2024-01-01`

**Example Requests:**

**Get last 3 months (JSON body):**
```bash
curl -X POST https://your-function-url \
  -H "Content-Type: application/json" \
  -d '{"months": 3}'
```

**Get from specific date (URL params):**
```bash
curl "https://your-function-url?start_date=2025-12-01"
```

**Custom tickers with date range:**
```bash
curl -X POST https://your-function-url \
  -H "Content-Type: application/json" \
  -d '{
    "tickers": "custom_tickers.csv",
    "start_date": "2025-11-15"
  }'
```

### Deduplication Strategy

**BigQuery is the single source of truth:**
1. On startup, loads all existing `transcript_id` values from BigQuery
2. If BigQuery fails to load → **aborts immediately** to prevent duplicates
3. **Heals orphans** — detects metadata rows missing content and backfills them automatically
4. Queries DuckDB for new transcripts within date range
5. Skips any transcript already in BigQuery (based on `transcript_id`)
6. Writes only new transcripts to BigQuery

**Result:** Safe to run multiple times without creating duplicate records. Orphaned data from partial failures is automatically repaired.

## Local Development & Usage

For local development, the tool runs in WSL (Windows Subsystem for Linux).

### Setup

**1. Configure Tickers**
Edit `tickers.csv`:
```csv
symbol
AAPL
MSFT
GOOGL
```

**2. Environment**
The `setup_and_run.sh` script automatically:
- Creates Python virtual environment
- Installs dependencies from `requirements.txt`
- Initializes local SQLite database
- Connects to BigQuery (if credentials available)

### Running Locally

**Via PowerShell (Windows):**

```powershell
# Default: Last 1 month
.\run_in_wsl.ps1

# Last 6 months
.\run_in_wsl.ps1 -months 6

# From specific start date
.\run_in_wsl.ps1 -start_date 2025-01-01
```

**Direct execution (WSL/Linux):**
```bash
./setup_and_run.sh --run_local --months 3
./setup_and_run.sh --run_local --start_date 2025-12-01
```

> **Note:** Local execution requires the `--run_local` flag as a safety check.

### Local Output

Local runs create three data stores:

1. **`transcripts.db`** (SQLite) - Normalized relational database
2. **`transcripts_metadata.csv`** - Metadata table (append mode)
3. **`transcripts_content.csv`** - Content table (append mode)

If BigQuery credentials are available, data is **also written to BigQuery** for synchronization.

## How DuckDB is Used

**DuckDB is the query engine, NOT the storage layer.**

The DefeatBeta API hosts earnings call transcript data on HuggingFace. To efficiently query this massive dataset:
- **DuckDB** executes SQL queries against the remote Parquet files
- Filters by ticker symbols and date ranges
- Returns matching transcripts for processing

**Why DuckDB queries still run:**
- It's the **data source** (input), not the storage (output)
- Cloud Run queries DuckDB → processes results → saves to BigQuery
- Thread count is optimized (1 thread in Cloud Run, 8 threads locally)

**Parquet Caching:**
- The HuggingFace parquet file is downloaded and cached locally in `/tmp/parquet_cache`
- Cache TTL is 1 hour — after expiry, a fresh copy is downloaded
- Prevents HTTP 429 (Too Many Requests) errors from repeated HuggingFace downloads
- Falls back to remote URL if the download fails

## ID Generation & Idempotency

**Transcript IDs are deterministic:**
```python
transcript_id = MD5(symbol + report_date)
```

**Benefits:**
- ✅ Same transcript generates same ID across all environments
- ✅ Enables deduplication without database lookups during ingestion
- ✅ Idempotent - re-running with same data doesn't create duplicates
- ✅ Consistent IDs for local SQLite, CSV files, and BigQuery

**Example:**
- `AAPL` on `2025-12-15` → Always generates `transcript_id: a1b2c3d4...`
- Re-processing this call 10 times → Still creates only 1 record in BigQuery

## CI/CD Pipeline

Deployment is automated via **Google Cloud Build** (`cloudbuild.yaml`):

1. **Build** - Docker image built with commit SHA and `latest` tags
2. **Push** - Image pushed to Artifact Registry (`us-central1-docker.pkg.dev`)
3. **Deploy** - Cloud Run service updated with new image (max 5 instances, concurrency 10)

Logging is set to `CLOUD_LOGGING_ONLY` to avoid requiring additional storage permissions.

## Troubleshooting

### Cloud Run Crashes with SIGABRT

**Symptom:**
```
terminate called without an active exception
Uncaught signal: 6, pid=8, tid=8
Worker (pid:8) was sent SIGABRT!
```

**Cause:** Insufficient memory allocation for DuckDB query processing.

**Solution:** Increase Cloud Run memory to 2GB:
```bash
gcloud run services update earnings-call-collector --memory 2Gi
```

### "Could not load BQ IDs" Error

**Symptom:**
```
Critical Error: Could not load BQ IDs: <error>
Aborting to prevent duplicate data insertion.
```

**Cause:** BigQuery connection failure or missing credentials.

**Solutions:**
1. Verify service account has BigQuery read/write permissions
2. Check `PROJECT_ID` and `DATASET_ID` are correct
3. Ensure BigQuery tables exist (run `setup_bq.py` once)

### No New Calls Found

**Symptom:**
```
Retrieved 4 potential matches. Processing for new calls...
No new calls found for local DB.
```

**Cause:** All transcripts already exist in BigQuery (working as intended).

**Solutions:**
- Expand date range: increase `months` or use earlier `start_date`
- Add more tickers to `tickers.csv`
- Check if you're running with very recent data (calls may not be published yet)

## License

MIT
