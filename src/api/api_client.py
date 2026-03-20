"""
api/api_client.py
-----------------
Simulates integration with Google Cloud APIs:
  - Google Sheets (read / write)
  - BigQuery (insert rows, run queries)

In production these would use:
  - google-cloud-bigquery Python client
  - gspread + google-auth for Sheets
"""

import logging
import json
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# GOOGLE SHEETS SIMULATION
# ─────────────────────────────────────────────

class GoogleSheetsClient:
    """
    Mock Google Sheets client.
    Simulates reading and writing data to a spreadsheet.
    In production, replace with: gspread.service_account(filename='creds.json')
    """

    def __init__(self, spreadsheet_id: str = "mock-sheet-id-001"):
        self.spreadsheet_id = spreadsheet_id
        # In-memory "sheet" storage (mimics a real spreadsheet)
        self._data: Dict[str, List[List[Any]]] = {
            "Sheet1": [
                ["user_id", "metric", "value", "updated_at"],
            ]
        }
        logger.info(f"GoogleSheetsClient initialised (sheet_id={spreadsheet_id})")

    def read(self, sheet_name: str = "Sheet1") -> List[List[Any]]:
        """Read all rows from a sheet tab."""
        logger.info(f"Reading from sheet: {self.spreadsheet_id}/{sheet_name}")
        rows = self._data.get(sheet_name, [])
        logger.info(f"Read {len(rows)} rows")
        return rows

    def write(self, rows: List[List[Any]], sheet_name: str = "Sheet1"):
        """Append rows to a sheet tab."""
        logger.info(f"Writing {len(rows)} rows to sheet: {self.spreadsheet_id}/{sheet_name}")
        if sheet_name not in self._data:
            self._data[sheet_name] = []
        self._data[sheet_name].extend(rows)
        logger.info("Write complete")

    def update_metrics(self, metrics: Dict[str, Any]):
        """Write a metrics summary row to Sheet1."""
        row = [
            metrics.get("user_id", "all"),
            metrics.get("metric", "summary"),
            metrics.get("value", 0),
            datetime.utcnow().isoformat(),
        ]
        self.write([row])


# ─────────────────────────────────────────────
# BIGQUERY SIMULATION
# ─────────────────────────────────────────────

class BigQueryClient:
    """
    Mock BigQuery client.
    Simulates inserting rows and running queries.
    In production, replace with: google.cloud.bigquery.Client()
    """

    def __init__(self, project_id: str = "my-gcp-project", dataset: str = "security_monitoring"):
        self.project_id = project_id
        self.dataset = dataset
        # Simulated table store
        self._tables: Dict[str, List[Dict]] = {}
        logger.info(f"BigQueryClient initialised (project={project_id}, dataset={dataset})")

    def _table_ref(self, table_name: str) -> str:
        return f"{self.project_id}.{self.dataset}.{table_name}"

    def insert_rows(self, table_name: str, rows: List[Dict]) -> bool:
        """
        Simulate streaming insert to BigQuery.
        In production: client.insert_rows_json(table_ref, rows)
        """
        ref = self._table_ref(table_name)
        logger.info(f"Inserting {len(rows)} rows into BigQuery table: {ref}")

        if table_name not in self._tables:
            self._tables[table_name] = []
        self._tables[table_name].extend(rows)

        logger.info(f"Insert successful. Table now has {len(self._tables[table_name])} rows")
        return True

    def query(self, sql: str) -> List[Dict]:
        """
        Simulate a BigQuery query job.
        Returns an empty result set (since we're mocking).
        In production: client.query(sql).result()
        """
        logger.info(f"Running BigQuery query: {sql[:80]}...")
        # Simulate latency / job ID
        job_id = f"bq-job-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        logger.info(f"Query job submitted: {job_id} (simulated)")
        return []

    def get_table_info(self, table_name: str) -> Dict:
        """Return metadata about a simulated table."""
        rows = self._tables.get(table_name, [])
        return {
            "table": self._table_ref(table_name),
            "row_count": len(rows),
            "last_updated": datetime.utcnow().isoformat(),
        }

    def list_tables(self) -> List[str]:
        """List all tables in the simulated dataset."""
        return list(self._tables.keys())


# ─────────────────────────────────────────────
# CONVENIENCE FUNCTIONS
# ─────────────────────────────────────────────

def push_metrics_to_sheets(metrics: Dict[str, Any]):
    """Push a metrics dict to the simulated Google Sheet."""
    client = GoogleSheetsClient()
    client.update_metrics(metrics)
    logger.info("Metrics pushed to Google Sheets (simulated)")


def push_metrics_to_bigquery(table_name: str, metrics: List[Dict]):
    """Push metrics rows to a simulated BigQuery table."""
    client = BigQueryClient()
    client.insert_rows(table_name, metrics)
    logger.info(f"Metrics pushed to BigQuery table '{table_name}' (simulated)")
