"""
tests/test_transformation.py
-----------------------------
Unit tests for the transformation module.
Uses small in-memory DataFrames — no file I/O needed.
"""

import os
import sys
import pytest
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.transformation.transform import (
    normalize_columns,
    remove_null_user_ids,
    remove_duplicates,
    parse_timestamps,
    compute_dau,
    compute_failed_login_rate,
    compute_sales_per_region,
    compute_suspicious_users,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_events():
    return pd.DataFrame({
        "user_id": ["u001", "u002", "u001", None, "u003"],
        "event_type": ["login", "purchase", "login", "login", "logout"],
        "event_time": [
            "2024-01-15 08:00:00",
            "2024-01-15 08:05:00",
            "2024-01-15 08:00:00",  # duplicate of row 0
            "2024-01-15 08:01:00",
            "2024-01-15 08:10:00",
        ],
        "ip_address": ["192.168.1.1", "10.0.0.1", "192.168.1.1", "1.1.1.1", "172.16.0.1"],
        "device": ["desktop", "mobile", "desktop", "unknown", "tablet"],
    })


@pytest.fixture
def sample_logins():
    return pd.DataFrame({
        "user_id": ["u001", "u002", "u002", "u002", "u003"],
        "login_time": [
            "2024-01-15 08:00:00",
            "2024-01-15 08:01:00",
            "2024-01-15 08:02:00",
            "2024-01-15 08:03:00",
            "2024-01-15 08:04:00",
        ],
        "success": [True, False, False, False, True],
        "location": ["New York", "Unknown", "Unknown", "Unknown", "Chicago"],
    })


@pytest.fixture
def sample_transactions():
    return pd.DataFrame({
        "user_id": ["u001", "u002", "u003"],
        "product_id": ["p101", "p102", "p101"],
        "amount": [99.99, 199.99, 49.99],
        "region": ["North", "South", "North"],
        "timestamp": ["2024-01-15 09:00:00", "2024-01-15 09:05:00", "2024-01-15 09:10:00"],
    })


# ─── Column Normalisation ─────────────────────────────────────────────────────

class TestNormalizeColumns:
    def test_lowercases_columns(self):
        df = pd.DataFrame({"UserID": [1], "EventType": [2]})
        result = normalize_columns(df)
        assert list(result.columns) == ["userid", "eventtype"]

    def test_replaces_spaces_with_underscores(self):
        df = pd.DataFrame({"User ID": [1], "Event Type": [2]})
        result = normalize_columns(df)
        assert "user_id" in result.columns
        assert "event_type" in result.columns

    def test_replaces_hyphens(self):
        df = pd.DataFrame({"user-id": [1]})
        result = normalize_columns(df)
        assert "user_id" in result.columns


# ─── Null Removal ─────────────────────────────────────────────────────────────

class TestRemoveNullUserIds:
    def test_removes_null_rows(self, sample_events):
        result = remove_null_user_ids(sample_events)
        assert result["user_id"].isna().sum() == 0

    def test_preserves_valid_rows(self, sample_events):
        result = remove_null_user_ids(sample_events)
        # Only 4 rows have non-null user_id
        assert len(result) == 4


# ─── Duplicate Removal ───────────────────────────────────────────────────────

class TestRemoveDuplicates:
    def test_removes_exact_duplicates(self, sample_events):
        # Row 0 and Row 2 are duplicates
        result = remove_duplicates(sample_events)
        assert len(result) < len(sample_events)

    def test_result_has_no_duplicates(self, sample_events):
        result = remove_duplicates(sample_events)
        assert result.duplicated().sum() == 0


# ─── Timestamp Parsing ────────────────────────────────────────────────────────

class TestParseTimestamps:
    def test_parses_valid_timestamps(self, sample_events):
        result = parse_timestamps(sample_events.copy(), "event_time")
        assert pd.api.types.is_datetime64_any_dtype(result["event_time"])

    def test_invalid_timestamps_become_nat(self):
        df = pd.DataFrame({"event_time": ["2024-01-15", "NOT_A_DATE", "2024-01-16"]})
        result = parse_timestamps(df, "event_time")
        assert result["event_time"].isna().sum() == 1


# ─── DAU Computation ─────────────────────────────────────────────────────────

class TestComputeDAU:
    def test_returns_dataframe(self, sample_events):
        events = parse_timestamps(sample_events.copy(), "event_time")
        result = compute_dau(events)
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, sample_events):
        events = parse_timestamps(sample_events.copy(), "event_time")
        result = compute_dau(events)
        assert "date" in result.columns
        assert "daily_active_users" in result.columns

    def test_dau_is_distinct_users(self, sample_events):
        events = parse_timestamps(remove_null_user_ids(sample_events.copy()), "event_time")
        result = compute_dau(events)
        # All events are on 2024-01-15, u001 appears twice → 3 distinct users
        assert result.iloc[0]["daily_active_users"] == 3


# ─── Failed Login Rate ────────────────────────────────────────────────────────

class TestComputeFailedLoginRate:
    def test_rate_is_between_0_and_1(self, sample_logins):
        logins = parse_timestamps(sample_logins.copy(), "login_time")
        result = compute_failed_login_rate(logins)
        assert (result["failed_login_rate"] >= 0).all()
        assert (result["failed_login_rate"] <= 1).all()

    def test_correct_failed_rate(self, sample_logins):
        # 3 out of 5 logins fail → rate = 0.6
        logins = parse_timestamps(sample_logins.copy(), "login_time")
        result = compute_failed_login_rate(logins)
        assert abs(result.iloc[0]["failed_login_rate"] - 0.6) < 0.01


# ─── Sales Per Region ────────────────────────────────────────────────────────

class TestComputeSalesPerRegion:
    def test_returns_per_region_totals(self, sample_transactions):
        result = compute_sales_per_region(sample_transactions)
        assert "region" in result.columns
        assert "total_sales" in result.columns

    def test_north_total(self, sample_transactions):
        result = compute_sales_per_region(sample_transactions)
        north = result[result["region"] == "North"]["total_sales"].iloc[0]
        assert abs(north - 149.98) < 0.01  # 99.99 + 49.99


# ─── Suspicious Users ────────────────────────────────────────────────────────

class TestComputeSuspiciousUsers:
    def test_flags_user_with_multiple_failures(self, sample_logins):
        logins = parse_timestamps(sample_logins.copy(), "login_time")
        result = compute_suspicious_users(logins, threshold=3)
        # u002 has 3 failed logins
        assert "u002" in result["user_id"].values

    def test_does_not_flag_below_threshold(self, sample_logins):
        logins = parse_timestamps(sample_logins.copy(), "login_time")
        # u001 and u003 have 0 and 0 failures — should not appear
        result = compute_suspicious_users(logins, threshold=3)
        assert "u001" not in result["user_id"].values

    def test_risk_level_high_for_many_failures(self, sample_logins):
        # Add more failures to push u002 over 5
        extra = pd.DataFrame({
            "user_id": ["u002", "u002", "u002"],
            "login_time": ["2024-01-15 09:00:00", "2024-01-15 09:01:00", "2024-01-15 09:02:00"],
            "success": [False, False, False],
            "location": ["Unknown", "Unknown", "Unknown"],
        })
        logins = pd.concat([sample_logins, extra], ignore_index=True)
        logins = parse_timestamps(logins, "login_time")
        result = compute_suspicious_users(logins, threshold=3)
        u002_risk = result[result["user_id"] == "u002"]["risk_level"].iloc[0]
        assert u002_risk == "HIGH"
