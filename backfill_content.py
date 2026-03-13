"""
Standalone script to backfill content rows for orphaned metadata entries in BigQuery.

An "orphan" is a transcript_id present in the metadata table but missing from the
content table — typically caused by a Cloud Run timeout after metadata was written
but before content was written.

Usage:
    python backfill_content.py
"""

import logging
from defeatbeta_api.client.duckdb_client import DuckDBClient, Configuration
from defeatbeta_api.client.hugging_face_client import HuggingFaceClient

import db_cloud_utils
from sql_get import _fetch_and_insert_orphan_content, PROJECT_ID, DATASET_ID

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    duckdb_client = DuckDBClient(log_level=logging.INFO, config=Configuration(threads=8))
    huggingface_client = HuggingFaceClient()

    try:
        logger.info("Checking for orphaned metadata rows in BigQuery...")
        orphans = db_cloud_utils.get_orphaned_metadata_bq(PROJECT_ID, DATASET_ID)

        if not orphans:
            print("No orphans found. BigQuery metadata and content tables are in sync.")
            return

        logger.info(f"Found {len(orphans)} orphaned transcript(s). Starting backfill...")
        _fetch_and_insert_orphan_content(duckdb_client, huggingface_client, orphans)
        print(f"Backfill complete. Processed {len(orphans)} orphaned transcript(s).")

    finally:
        try:
            duckdb_client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
