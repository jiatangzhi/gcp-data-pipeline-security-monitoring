"""
tests/test_validation.py
------------------------
Unit tests for the validation module.
Each test exercises a specific check in isolation.
"""

import os
import sys
import pytest
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.validation.validate import (
    check_no_null_user_ids,
    check_no_duplicates,
    check_valid_timestamps,
    check_row_count,
    check_amount_positive,
    validate_all,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def clean_df():
    """A fully clean DataFrame that should pass all checks."""
    return pd.DataFrame({
        "user_id": ["u001", "u002", "u003"],
        "event_time": pd.to_datetime(["2024-01-15 08:00:00", "2024-01-15 08:05:00", "2024-01-15 08:10:00"]),
        "amount": [99.99, 49.99, 199.99],
    })


@pytest.fixture
def df_with_null_user():
    return pd.DataFrame({
        "user_id": ["u001", None, "u003"],
        "event_time": pd.to_datetime(["2024-01-15", "2024-01-15", "2024-01-15"]),
    })


@pytest.fixture
def df_with_duplicates():
    return pd.DataFrame({
        "user_id": ["u001", "u001", "u002"],
        "event_time": pd.to_datetime(["2024-01-15", "2024-01-15", "2024-01-16"]),
    })


@pytest.fixture
def df_with_bad_timestamps():
    df = pd.DataFrame({
        "user_id": ["u001", "u002"],
        "event_time": [pd.Timestamp("2024-01-15"), pd.NaT],
    })
    return df


@pytest.fixture
def df_empty():
    return pd.DataFrame({"user_id": [], "event_time": []})


# ─── check_no_null_user_ids ───────────────────────────────────────────────────

class TestCheckNoNullUserIds:
    def test_passes_with_clean_data(self, clean_df):
        result = check_no_null_user_ids(clean_df, "test")
        assert result["passed"] is True

    def test_fails_with_null_user_id(self, df_with_null_user):
        result = check_no_null_user_ids(df_with_null_user, "test")
        assert result["passed"] is False
        assert result["details"]["null_count"] == 1

    def test_result_has_required_keys(self, clean_df):
        result = check_no_null_user_ids(clean_df, "test")
        for key in ["check", "dataset", "passed", "message", "details"]:
            assert key in result


# ─── check_no_duplicates ─────────────────────────────────────────────────────

class TestCheckNoDuplicates:
    def test_passes_with_unique_rows(self, clean_df):
        result = check_no_duplicates(clean_df, "test")
        assert result["passed"] is True

    def test_fails_with_duplicate_rows(self, df_with_duplicates):
        result = check_no_duplicates(df_with_duplicates, "test")
        assert result["passed"] is False
        assert result["details"]["duplicate_count"] >= 1


# ─── check_valid_timestamps ───────────────────────────────────────────────────

class TestCheckValidTimestamps:
    def test_passes_with_valid_timestamps(self, clean_df):
        result = check_valid_timestamps(clean_df, "test", "event_time")
        assert result["passed"] is True

    def test_fails_with_nat_timestamps(self, df_with_bad_timestamps):
        result = check_valid_timestamps(df_with_bad_timestamps, "test", "event_time")
        assert result["passed"] is False
        assert result["details"]["invalid_count"] == 1

    def test_fails_when_column_missing(self, clean_df):
        result = check_valid_timestamps(clean_df, "test", "nonexistent_col")
        assert result["passed"] is False
        assert "not found" in result["message"]


# ─── check_row_count ──────────────────────────────────────────────────────────

class TestCheckRowCount:
    def test_passes_with_rows_above_minimum(self, clean_df):
        result = check_row_count(clean_df, "test", min_rows=1)
        assert result["passed"] is True

    def test_fails_when_empty(self, df_empty):
        result = check_row_count(df_empty, "test", min_rows=1)
        assert result["passed"] is False

    def test_fails_when_below_minimum(self, clean_df):
        result = check_row_count(clean_df, "test", min_rows=100)
        assert result["passed"] is False


# ─── check_amount_positive ───────────────────────────────────────────────────

class TestCheckAmountPositive:
    def test_passes_with_positive_amounts(self, clean_df):
        result = check_amount_positive(clean_df)
        assert result["passed"] is True

    def test_fails_with_negative_amount(self):
        df = pd.DataFrame({"user_id": ["u001"], "amount": [-5.0]})
        result = check_amount_positive(df)
        assert result["passed"] is False
        assert result["details"]["negative_count"] == 1

    def test_fails_with_zero_amount(self):
        df = pd.DataFrame({"user_id": ["u001"], "amount": [0.0]})
        result = check_amount_positive(df)
        assert result["passed"] is False

    def test_fails_when_amount_column_missing(self):
        df = pd.DataFrame({"user_id": ["u001"]})
        result = check_amount_positive(df)
        assert result["passed"] is False


# ─── validate_all ─────────────────────────────────────────────────────────────

class TestValidateAll:
    @pytest.fixture
    def clean_data(self):
        events = pd.DataFrame({
            "user_id": ["u001", "u002"],
            "event_time": pd.to_datetime(["2024-01-15 08:00:00", "2024-01-15 08:05:00"]),
        })
        logins = pd.DataFrame({
            "user_id": ["u001", "u002"],
            "login_time": pd.to_datetime(["2024-01-15 08:00:00", "2024-01-15 08:01:00"]),
        })
        transactions = pd.DataFrame({
            "user_id": ["u001", "u002"],
            "timestamp": pd.to_datetime(["2024-01-15 09:00:00", "2024-01-15 09:05:00"]),
            "amount": [99.99, 49.99],
        })
        return {"events": events, "logins": logins, "transactions": transactions}

    def test_returns_summary_dict(self, clean_data):
        result = validate_all(clean_data)
        assert isinstance(result, dict)
        assert "all_passed" in result
        assert "results" in result

    def test_clean_data_passes_all_checks(self, clean_data):
        result = validate_all(clean_data)
        assert result["all_passed"] is True

    def test_summary_has_counts(self, clean_data):
        result = validate_all(clean_data)
        assert result["total_checks"] > 0
        assert result["passed"] == result["total_checks"]
        assert result["failed"] == 0
