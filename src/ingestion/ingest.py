"""
ingestion/ingest.py
-------------------
Two ingestion modes, selected via the INGESTION_MODE env var:

  csv      (default) — reads from local /data CSV files (batch, dev/testing)
  pubsub             — pulls messages from a Google Cloud Pub/Sub subscription
                       (event-driven, production pattern)

Entry point for the Airflow DAG: ingest_all()
"""

import os
import json
import base64
import logging
import pandas as pd
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────

DATA_DIR       = os.path.join(os.path.dirname(__file__), "..", "..", "data")
INGESTION_MODE = os.environ.get("INGESTION_MODE", "csv")   # "csv" | "pubsub"
GCP_PROJECT    = os.environ.get("GCP_PROJECT", "my-gcp-project")

# Pub/Sub subscription IDs (one per data source)
PUBSUB_SUBSCRIPTIONS = {
    "events":       os.environ.get("PUBSUB_SUB_EVENTS",       "events-sub"),
    "logins":       os.environ.get("PUBSUB_SUB_LOGINS",       "logins-sub"),
    "transactions": os.environ.get("PUBSUB_SUB_TRANSACTIONS", "transactions-sub"),
}

# Max messages to pull per subscription per run
PUBSUB_MAX_MESSAGES = int(os.environ.get("PUBSUB_MAX_MESSAGES", "1000"))


# ─── CSV ingestion (batch / local) ───────────────────────────────────────────

def load_csv(filename: str) -> pd.DataFrame:
    """Load a CSV file from the /data directory into a DataFrame."""
    filepath = os.path.join(DATA_DIR, filename)
    logger.info(f"Loading CSV: {filepath}")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Data file not found: {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df)} rows from {filename}")
    return df


def ingest_events_csv() -> pd.DataFrame:
    return load_csv("events.csv")


def ingest_logins_csv() -> pd.DataFrame:
    return load_csv("logins.csv")


def ingest_transactions_csv() -> pd.DataFrame:
    return load_csv("transactions.csv")


# ─── Pub/Sub ingestion (event-driven / production) ───────────────────────────

def pull_pubsub(subscription_id: str) -> pd.DataFrame:
    """
    Pull messages from a Pub/Sub subscription and return them as a DataFrame.

    Production pattern:
      - Each message carries one event as a JSON payload in data (base64-encoded)
      - Messages are acknowledged after successful processing (at-least-once delivery)
      - In production, use a push subscription + Cloud Run endpoint for real-time

    Auth: ADC (service account on Cloud Run, gcloud locally)
    """
    try:
        from google.cloud import pubsub_v1
    except ImportError:
        logger.warning("google-cloud-pubsub not installed — falling back to CSV")
        return None

    subscription_path = (
        f"projects/{GCP_PROJECT}/subscriptions/{subscription_id}"
    )
    logger.info(f"Pulling from Pub/Sub: {subscription_path}")

    subscriber = pubsub_v1.SubscriberClient()
    records = []
    ack_ids = []

    try:
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages":  PUBSUB_MAX_MESSAGES,
            }
        )

        if not response.received_messages:
            logger.info(f"No messages in {subscription_id}")
            return pd.DataFrame()

        for msg in response.received_messages:
            ack_ids.append(msg.ack_id)
            # Pub/Sub message data is base64-encoded JSON
            payload = json.loads(base64.b64decode(msg.message.data).decode("utf-8"))
            records.append(payload)

        # Acknowledge — tell Pub/Sub we processed the messages
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )

        df = pd.DataFrame(records)
        logger.info(f"Pulled and acked {len(df)} messages from {subscription_id}")
        return df

    except Exception as e:
        logger.error(f"Pub/Sub pull failed for {subscription_id}: {e}")
        raise
    finally:
        subscriber.close()


def ingest_events_pubsub() -> pd.DataFrame:
    return pull_pubsub(PUBSUB_SUBSCRIPTIONS["events"])


def ingest_logins_pubsub() -> pd.DataFrame:
    return pull_pubsub(PUBSUB_SUBSCRIPTIONS["logins"])


def ingest_transactions_pubsub() -> pd.DataFrame:
    return pull_pubsub(PUBSUB_SUBSCRIPTIONS["transactions"])


# ─── API simulation (unchanged) ──────────────────────────────────────────────

def simulate_api_fetch(endpoint: str, params: dict = None) -> dict:
    """
    Simulate fetching metadata from an external API.
    Uses httpbin.org as a mock endpoint in dev/testing.
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
        return {
            "source": "mock_api",
            "timestamp": datetime.utcnow().isoformat(),
            "records": 0,
            "status": "simulated",
        }


# ─── Main entry point ────────────────────────────────────────────────────────

def ingest_all() -> dict:
    """
    Run all ingestion steps and return a dict of DataFrames.
    Called by the Airflow DAG extract_task.

    Switches between CSV and Pub/Sub based on INGESTION_MODE env var:
      INGESTION_MODE=csv     → reads local CSV files   (default)
      INGESTION_MODE=pubsub  → pulls from Pub/Sub subscriptions
    """
    logger.info(f"=== Starting ingestion phase (mode: {INGESTION_MODE}) ===")

    if INGESTION_MODE == "pubsub":
        data = {
            "events":       ingest_events_pubsub(),
            "logins":       ingest_logins_pubsub(),
            "transactions": ingest_transactions_pubsub(),
        }
    else:
        data = {
            "events":       ingest_events_csv(),
            "logins":       ingest_logins_csv(),
            "transactions": ingest_transactions_csv(),
        }

    api_response = simulate_api_fetch(
        "https://httpbin.org/get", params={"source": "gcp-pipeline"}
    )
    logger.info(f"API simulation complete. Status: {api_response.get('status', 'ok')}")

    logger.info("=== Ingestion phase complete ===")
    return data
