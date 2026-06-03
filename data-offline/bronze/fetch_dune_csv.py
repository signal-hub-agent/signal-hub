import os
import logging
from pathlib import Path
from dune_client.client import DuneClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DuneBronzeFetcher')

DUNE_API_KEY = os.getenv("DUNE_API_KEY", "")
QUERY_ID = 7595616

# Resolve paths to ensure script can be run from anywhere
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CSV_PATH = DATA_DIR / "dune_30d_swaps.csv"


def fetch_and_store_dune_data():
    """
    Fetches raw execution results from Dune Analytics and persists them as a CSV file.
    Acts as the Bronze layer ingestion for historical cold data.
    """
    if not DUNE_API_KEY:
        logger.error("DUNE_API_KEY environment variable is not set. Aborting ingestion.")
        raise RuntimeError("Missing DUNE_API_KEY configuration.")

    logger.info("Initializing Dune Client for Query ID: %d", QUERY_ID)
    dune = DuneClient(DUNE_API_KEY)

    try:
        df = dune.get_latest_result_dataframe(QUERY_ID)

        if df.empty:
            logger.warning("Dune query returned an empty dataset.")
            return

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(CSV_PATH, index=False)

        logger.info("Successfully fetched and saved %d records to %s", len(df), CSV_PATH)

    except Exception as exc:
        logger.error("Failed to fetch or save Dune query results: %s", str(exc), exc_info=True)
        raise


if __name__ == "__main__":
    fetch_and_store_dune_data()