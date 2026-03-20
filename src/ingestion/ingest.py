"""
ingestion/ingest.py
-------------------
Handles data ingestion from CSV files and simulated API sources.
In a real GCP environment, this would pull from Cloud Storage or Pub/Sub.
"""

import os
import logging
import pandas as pd
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

# Path to the data directory (relative to project root)
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def load_csv(filename: str) -> pd.DataFrame:
    """
    Load a CSV file from the /data directory into a pandas DataFrame.
    """
    filepath = os.path.join(DATA_DIR, filename)
    logger.info(f"Loading CSV: {filepath}")

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Data file not found: {filepath}")

    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df)} rows from {filename}")
    return df


def ingest_events() -> pd.DataFrame:
    """Load user activity events."""
    return load_csv("events.csv")


def ingest_logins() -> pd.DataFrame:
    """Load login attempts."""
    return load_csv("logins.csv")


def ingest_transactions() -> pd.DataFrame:
    """Load transactions."""
    return load_csv("transactions.csv")


def simulate_api_fetch(endpoint: str, params: dict = None) -> dict:
    """
    Simulate fetching data from an external API.
    In production, this would call a real GCP API or internal service.

    We use httpbin.org as a mock endpoint that echoes our request back.
    """
    logger.info(f"Simulating API call to: {endpoint}")
    try:
        response = requests.get(endpoint, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()
        logger.info(f"API call successful, got {len(data)} fields in response")
        return data
    except requests.exceptions.RequestException as e:
        logger.warning(f"API call failed (using mock fallback): {e}")
        # Return mock data so the pipeline doesn't break in offline mode
        return {
            "source": "mock_api",
            "timestamp": datetime.utcnow().isoformat(),
            "records": 0,
            "status": "simulated",
        }


def ingest_all() -> dict:
    """
    Run all ingestion steps and return a dict of DataFrames.
    This is the main entry point called by the Airflow DAG.
    """
    logger.info("=== Starting ingestion phase ===")

    data = {
        "events": ingest_events(),
        "logins": ingest_logins(),
        "transactions": ingest_transactions(),
    }

    # Simulate an API call (e.g., fetching metadata from an internal service)
    api_response = simulate_api_fetch("https://httpbin.org/get", params={"source": "gcp-pipeline"})
    logger.info(f"API simulation complete. Status: {api_response.get('status', 'ok')}")

    logger.info("=== Ingestion phase complete ===")
    return data
