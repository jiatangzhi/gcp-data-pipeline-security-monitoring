"""
tests/test_ingestion.py
-----------------------
Unit tests for the ingestion module.
Tests are self-contained: they use the real CSV files from /data.
"""

import os
import sys
import pytest
import pandas as pd

# Make sure the project root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ingestion.ingest import load_csv, ingest_events, ingest_logins, ingest_transactions


class TestLoadCsv:
    """Tests for the generic CSV loader."""

    def test_load_events_returns_dataframe(self):
        df = load_csv("events.csv")
        assert isinstance(df, pd.DataFrame)

    def test_load_events_has_expected_columns(self):
        df = load_csv("events.csv")
        expected = {"user_id", "event_type", "event_time", "ip_address", "device"}
        assert expected.issubset(set(df.columns))

    def test_load_events_has_rows(self):
        df = load_csv("events.csv")
        assert len(df) > 0

    def test_load_nonexistent_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_csv("nonexistent_file.csv")


class TestIngestEvents:
    def test_returns_dataframe(self):
        df = ingest_events()
        assert isinstance(df, pd.DataFrame)

    def test_has_user_id_column(self):
        df = ingest_events()
        assert "user_id" in df.columns

    def test_has_event_type_column(self):
        df = ingest_events()
        assert "event_type" in df.columns


class TestIngestLogins:
    def test_returns_dataframe(self):
        df = ingest_logins()
        assert isinstance(df, pd.DataFrame)

    def test_has_success_column(self):
        df = ingest_logins()
        assert "success" in df.columns

    def test_has_location_column(self):
        df = ingest_logins()
        assert "location" in df.columns


class TestIngestTransactions:
    def test_returns_dataframe(self):
        df = ingest_transactions()
        assert isinstance(df, pd.DataFrame)

    def test_has_amount_column(self):
        df = ingest_transactions()
        assert "amount" in df.columns

    def test_has_region_column(self):
        df = ingest_transactions()
        assert "region" in df.columns

    def test_amounts_are_numeric(self):
        df = ingest_transactions()
        assert pd.api.types.is_numeric_dtype(df["amount"])
