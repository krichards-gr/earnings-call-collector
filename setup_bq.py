from db_cloud_utils import initialize_bq
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ID = "sri-benchmarking-databases"
DATASET_ID = "pressure_monitoring"

if __name__ == "__main__":
    print(f"Initializing BigQuery tables in {PROJECT_ID}.{DATASET_ID}...")
    try:
        initialize_bq(PROJECT_ID, DATASET_ID)
        print("Initialization complete.")
    except Exception as e:
        print(f"Initialization failed: {e}")
