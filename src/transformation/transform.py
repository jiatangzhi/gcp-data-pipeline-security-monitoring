"""
transformation/transform.py
---------------------------
Cleans raw data and computes security + analytics metrics.
Follows the pattern: clean → enrich → aggregate.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CLEANING HELPERS
# ─────────────────────────────────────────────

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all column names to snake_case and strip whitespace."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df


def remove_null_user_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where user_id is null — these can't be attributed to anyone."""
    before = len(df)
    df = df.dropna(subset=["user_id"])
    dropped = before - len(df)
    if dropped:
        logger.warning(f"Dropped {dropped} rows with null user_id")
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact duplicate rows."""
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    if dropped:
        logger.info(f"Removed {dropped} duplicate rows")
    return df


def parse_timestamps(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    """Parse a timestamp column to datetime, coercing errors to NaT."""
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    invalid = df[time_col].isna().sum()
    if invalid:
        logger.warning(f"Found {invalid} unparseable timestamps in '{time_col}', set to NaT")
    return df


def clean_dataframe(df: pd.DataFrame, time_col: str = None) -> pd.DataFrame:
    """Apply all standard cleaning steps to a DataFrame."""
    df = normalize_columns(df)
    df = remove_null_user_ids(df)
    df = remove_duplicates(df)
    if time_col:
        df = parse_timestamps(df, time_col)
    return df


# ─────────────────────────────────────────────
# METRIC COMPUTATION
# ─────────────────────────────────────────────

def compute_dau(events: pd.DataFrame) -> pd.DataFrame:
    """
    Daily Active Users (DAU): count distinct users per day.
    A user is 'active' if they generated any event on that day.
    """
    events = events.copy()
    events["date"] = events["event_time"].dt.date
    dau = (
        events.groupby("date")["user_id"]
        .nunique()
        .reset_index(name="daily_active_users")
    )
    logger.info(f"Computed DAU for {len(dau)} days")
    return dau


def compute_failed_login_rate(logins: pd.DataFrame) -> pd.DataFrame:
    """
    Failed login rate per day: (failed logins) / (total logins).
    """
    logins = logins.copy()
    logins["date"] = logins["login_time"].dt.date

    # 'success' column may be bool or string — normalise to bool
    logins["success"] = logins["success"].astype(str).str.lower().map(
        {"true": True, "false": False, "1": True, "0": False}
    )

    daily = logins.groupby("date").agg(
        total_logins=("user_id", "count"),
        failed_logins=("success", lambda x: (~x).sum()),
    ).reset_index()

    daily["failed_login_rate"] = (
        daily["failed_logins"] / daily["total_logins"]
    ).round(4)

    logger.info("Computed failed login rate")
    return daily


def compute_sales_per_region(transactions: pd.DataFrame) -> pd.DataFrame:
    """Total sales revenue grouped by region."""
    sales = (
        transactions.groupby("region")["amount"]
        .sum()
        .reset_index(name="total_sales")
    )
    sales["total_sales"] = sales["total_sales"].round(2)
    logger.info(f"Computed sales for {len(sales)} regions")
    return sales


def compute_suspicious_users(logins: pd.DataFrame, threshold: int = 3) -> pd.DataFrame:
    """
    Flag users with >= `threshold` failed login attempts.
    These are candidates for security review.
    """
    logins = logins.copy()
    logins["success"] = logins["success"].astype(str).str.lower().map(
        {"true": True, "false": False, "1": True, "0": False}
    )

    failed = logins[logins["success"] == False]
    counts = (
        failed.groupby("user_id")
        .size()
        .reset_index(name="failed_attempts")
    )
    suspicious = counts[counts["failed_attempts"] >= threshold].copy()
    suspicious["risk_level"] = suspicious["failed_attempts"].apply(
        lambda x: "HIGH" if x >= 5 else "MEDIUM"
    )

    logger.warning(f"Found {len(suspicious)} suspicious users (threshold={threshold})")
    return suspicious


# ─────────────────────────────────────────────
# MAIN TRANSFORMATION ENTRY POINT
# ─────────────────────────────────────────────

def transform_all(raw_data: dict) -> dict:
    """
    Run all transformation steps.
    Input:  raw_data dict with keys 'events', 'logins', 'transactions'
    Output: dict with 'cleaned' and 'aggregated' sub-dicts
    """
    logger.info("=== Starting transformation phase ===")

    # --- Clean each dataset ---
    events = clean_dataframe(raw_data["events"], time_col="event_time")
    logins = clean_dataframe(raw_data["logins"], time_col="login_time")
    transactions = clean_dataframe(raw_data["transactions"], time_col="timestamp")

    # --- Compute metrics ---
    dau = compute_dau(events)
    failed_rate = compute_failed_login_rate(logins)
    sales = compute_sales_per_region(transactions)
    suspicious = compute_suspicious_users(logins)

    result = {
        "cleaned": {
            "events": events,
            "logins": logins,
            "transactions": transactions,
        },
        "aggregated": {
            "dau": dau,
            "failed_login_rate": failed_rate,
            "sales_per_region": sales,
            "suspicious_users": suspicious,
        },
    }

    logger.info("=== Transformation phase complete ===")
    return result
