"""
warehouse/warehouse.py
----------------------
Data warehouse layer using Google BigQuery.
Loads raw, cleaned, and aggregated DataFrames into BigQuery tables
via the google-cloud-bigquery Python client.

Auth is handled by Application Default Credentials (ADC):
  - Locally:    gcloud auth application-default login
  - Cloud Run:  attached service account — no extra config needed

Environment variables (set via .env or Cloud Run):
  GCP_PROJECT  — GCP project ID
  BQ_DATASET   — BigQuery dataset ID (default: security_monitoring)

Table layout:
  raw_events / raw_logins / raw_transactions          (raw ingested data)
  clean_events / clean_logins / clean_transactions    (cleaned data)
  agg_dau / agg_failed_login_rate / agg_sales_per_region / agg_suspicious_users
"""

import os
import logging
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT", "my-gcp-project")
DATASET_ID = os.environ.get("BQ_DATASET", "security_monitoring")


class DataWarehouse:
    """
    Manages writes and reads to a Google BigQuery dataset.
    Follows a raw → cleaned → aggregated warehouse pattern.
    """

    def __init__(self, project_id: str = PROJECT_ID, dataset_id: str = DATASET_ID):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client: bigquery.Client | None = None

    # ─── Helpers ─────────────────────────────────────────────────────────────

    def _table_ref(self, table_name: str) -> str:
        """Fully-qualified BigQuery table id: project.dataset.table"""
        return f"{self.project_id}.{self.dataset_id}.{table_name}"

    # ─── Connection ──────────────────────────────────────────────────────────

    def connect(self) -> "DataWarehouse":
        logger.info(f"Connecting to BigQuery: {self.project_id}.{self.dataset_id}")
        self.client = bigquery.Client(project=self.project_id)
        logger.info("BigQuery client ready")
        return self

    def close(self):
        if self.client:
            self.client.close()
            logger.info("BigQuery client closed")

    def __enter__(self):
        return self.connect()

    def __exit__(self, *args):
        self.close()

    # ─── Core loader ─────────────────────────────────────────────────────────

    def _load_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Upload a DataFrame to BigQuery with WRITE_TRUNCATE (full refresh).
        Schema is auto-detected from the DataFrame dtypes.
        Blocks until the load job completes.
        """
        table_id = self._table_ref(table_name)
        logger.info(f"Loading {len(df)} rows → {table_id}")

        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Block until complete; raises on failure

        logger.info(f"  '{table_name}': {job.output_rows} rows written")

    # ─── RAW LAYER ───────────────────────────────────────────────────────────

    def load_raw(self, df: pd.DataFrame, table_name: str):
        self._load_dataframe(df, table_name)

    def load_raw_all(self, raw_data: dict):
        self.load_raw(raw_data["events"], "raw_events")
        self.load_raw(raw_data["logins"], "raw_logins")
        self.load_raw(raw_data["transactions"], "raw_transactions")

    # ─── CLEAN LAYER ─────────────────────────────────────────────────────────

    def load_cleaned(self, cleaned_data: dict):
        for name, df in cleaned_data.items():
            self._load_dataframe(df, f"clean_{name}")

    # ─── AGGREGATED LAYER ────────────────────────────────────────────────────

    def load_aggregated(self, aggregated_data: dict):
        for name, df in aggregated_data.items():
            self._load_dataframe(df, f"agg_{name}")

    # ─── SQL ANALYTICS ───────────────────────────────────────────────────────

    def query(self, sql: str) -> pd.DataFrame:
        """Run a SQL query against BigQuery and return a DataFrame."""
        logger.debug(f"Executing query: {sql[:120]}...")
        return self.client.query(sql).result().to_dataframe()

    def get_dau(self) -> pd.DataFrame:
        return self.query(f"""
            SELECT * FROM `{self._table_ref('agg_dau')}`
            ORDER BY date
        """)

    def get_top_products(self, n: int = 5) -> pd.DataFrame:
        return self.query(f"""
            SELECT
                product_id,
                COUNT(*)                AS total_orders,
                ROUND(SUM(amount), 2)   AS total_revenue
            FROM `{self._table_ref('clean_transactions')}`
            GROUP BY product_id
            ORDER BY total_revenue DESC
            LIMIT {n}
        """)

    def get_failed_login_rate(self) -> pd.DataFrame:
        return self.query(f"""
            SELECT * FROM `{self._table_ref('agg_failed_login_rate')}`
            ORDER BY date
        """)

    def get_suspicious_users(self) -> pd.DataFrame:
        return self.query(f"""
            SELECT * FROM `{self._table_ref('agg_suspicious_users')}`
            ORDER BY failed_attempts DESC
        """)

    def get_sales_per_region(self) -> pd.DataFrame:
        return self.query(f"""
            SELECT * FROM `{self._table_ref('agg_sales_per_region')}`
            ORDER BY total_sales DESC
        """)

    def list_tables(self) -> list:
        """Return all table names in the dataset."""
        tables = self.client.list_tables(f"{self.project_id}.{self.dataset_id}")
        return [t.table_id for t in tables]

    def print_summary(self):
        """Print row counts for every table in the dataset."""
        logger.info("=== Warehouse Summary ===")
        for table_name in self.list_tables():
            meta = self.client.get_table(self._table_ref(table_name))
            logger.info(f"  {table_name}: {meta.num_rows} rows")


def load_to_warehouse(raw_data: dict, transformed_data: dict):
    """
    Main entry point for the warehouse loading phase.
    Called by the Airflow DAG's load_task.
    """
    logger.info("=== Starting warehouse load phase ===")

    with DataWarehouse() as wh:
        wh.load_raw_all(raw_data)
        wh.load_cleaned(transformed_data["cleaned"])
        wh.load_aggregated(transformed_data["aggregated"])

        wh.print_summary()

        logger.info("--- Key Metrics ---")
        logger.info(f"DAU:\n{wh.get_dau().to_string(index=False)}")
        logger.info(f"Top Products:\n{wh.get_top_products().to_string(index=False)}")
        logger.info(f"Suspicious Users:\n{wh.get_suspicious_users().to_string(index=False)}")
        logger.info(f"Sales by Region:\n{wh.get_sales_per_region().to_string(index=False)}")

    logger.info("=== Warehouse load phase complete ===")
