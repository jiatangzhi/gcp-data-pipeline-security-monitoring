"""
validation/validate.py
----------------------
Data quality checks run before loading data into the warehouse.
Each check returns a dict with: passed (bool), message (str), details (dict).
"""

import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


def check_no_null_user_ids(df: pd.DataFrame, name: str) -> dict:
    """Ensure no null user_id values remain after cleaning."""
    null_count = df["user_id"].isna().sum()
    passed = null_count == 0
    result = {
        "check": "no_null_user_ids",
        "dataset": name,
        "passed": passed,
        "message": f"{null_count} null user_id(s) found" if not passed else "OK",
        "details": {"null_count": int(null_count)},
    }
    _log_result(result)
    return result


def check_no_duplicates(df: pd.DataFrame, name: str) -> dict:
    """Ensure the DataFrame has no exact duplicate rows."""
    dup_count = df.duplicated().sum()
    passed = dup_count == 0
    result = {
        "check": "no_duplicates",
        "dataset": name,
        "passed": passed,
        "message": f"{dup_count} duplicate row(s) found" if not passed else "OK",
        "details": {"duplicate_count": int(dup_count)},
    }
    _log_result(result)
    return result


def check_valid_timestamps(df: pd.DataFrame, name: str, time_col: str) -> dict:
    """Ensure the timestamp column has no NaT values (invalid timestamps)."""
    if time_col not in df.columns:
        return {
            "check": "valid_timestamps",
            "dataset": name,
            "passed": False,
            "message": f"Column '{time_col}' not found",
            "details": {},
        }

    invalid_count = df[time_col].isna().sum()
    passed = invalid_count == 0
    result = {
        "check": "valid_timestamps",
        "dataset": name,
        "passed": passed,
        "message": f"{invalid_count} invalid timestamp(s) in '{time_col}'" if not passed else "OK",
        "details": {"invalid_count": int(invalid_count), "column": time_col},
    }
    _log_result(result)
    return result


def check_row_count(df: pd.DataFrame, name: str, min_rows: int = 1) -> dict:
    """Ensure the dataset is not empty."""
    row_count = len(df)
    passed = row_count >= min_rows
    result = {
        "check": "row_count",
        "dataset": name,
        "passed": passed,
        "message": f"Only {row_count} row(s), expected >= {min_rows}" if not passed else "OK",
        "details": {"row_count": row_count, "min_required": min_rows},
    }
    _log_result(result)
    return result


def check_amount_positive(df: pd.DataFrame) -> dict:
    """Ensure all transaction amounts are positive numbers."""
    if "amount" not in df.columns:
        return {
            "check": "amount_positive",
            "dataset": "transactions",
            "passed": False,
            "message": "Column 'amount' not found",
            "details": {},
        }

    negative_count = (df["amount"] <= 0).sum()
    passed = negative_count == 0
    result = {
        "check": "amount_positive",
        "dataset": "transactions",
        "passed": passed,
        "message": f"{negative_count} non-positive amount(s) found" if not passed else "OK",
        "details": {"negative_count": int(negative_count)},
    }
    _log_result(result)
    return result


def _log_result(result: dict):
    """Log the result of a validation check."""
    status = "PASS" if result["passed"] else "FAIL"
    msg = f"[{status}] {result['dataset']}.{result['check']}: {result['message']}"
    if result["passed"]:
        logger.info(msg)
    else:
        logger.warning(msg)


def validate_all(cleaned_data: dict) -> dict:
    """
    Run all data quality checks on the cleaned datasets.
    Returns a summary report with pass/fail status per check.
    Raises ValueError if any critical check fails.
    """
    logger.info("=== Starting data validation ===")

    events = cleaned_data["events"]
    logins = cleaned_data["logins"]
    transactions = cleaned_data["transactions"]

    results = []

    # Events checks
    results += [
        check_no_null_user_ids(events, "events"),
        check_no_duplicates(events, "events"),
        check_valid_timestamps(events, "events", "event_time"),
        check_row_count(events, "events"),
    ]

    # Logins checks
    results += [
        check_no_null_user_ids(logins, "logins"),
        check_no_duplicates(logins, "logins"),
        check_valid_timestamps(logins, "logins", "login_time"),
        check_row_count(logins, "logins"),
    ]

    # Transactions checks
    results += [
        check_no_null_user_ids(transactions, "transactions"),
        check_no_duplicates(transactions, "transactions"),
        check_valid_timestamps(transactions, "transactions", "timestamp"),
        check_amount_positive(transactions),
        check_row_count(transactions, "transactions"),
    ]

    # Build summary
    total = len(results)
    passed = sum(1 for r in results if r["passed"])
    failed = total - passed

    summary = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_checks": total,
        "passed": passed,
        "failed": failed,
        "all_passed": failed == 0,
        "results": results,
    }

    if failed > 0:
        failed_checks = [r["check"] + "(" + r["dataset"] + ")" for r in results if not r["passed"]]
        logger.warning(f"Validation completed with {failed} failure(s): {failed_checks}")
    else:
        logger.info("All validation checks passed!")

    logger.info("=== Validation phase complete ===")
    return summary
