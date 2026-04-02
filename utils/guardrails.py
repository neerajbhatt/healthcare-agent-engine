"""Guardrails for PHI/PII protection and query safety."""

from __future__ import annotations

import re
from typing import Any

from utils.logging import get_logger

logger = get_logger(__name__)

# ── Dangerous SQL patterns ──
BLOCKED_SQL_PATTERNS = [
    r"\bDROP\s+(TABLE|DATABASE|SCHEMA)\b",
    r"\bDELETE\s+FROM\b",
    r"\bTRUNCATE\b",
    r"\bALTER\s+TABLE\b",
    r"\bCREATE\s+(TABLE|DATABASE)\b",
    r"\bINSERT\s+INTO\b",
    r"\bUPDATE\s+\w+\s+SET\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
]

# ── PII column patterns (should not appear in output) ──
PII_COLUMNS = {
    "ssn", "social_security", "member_ssn",
    "date_of_birth", "dob", "birth_date",
    "street_address", "address_line_1", "address_line_2",
    "phone_number", "phone", "email", "email_address",
    "first_name", "last_name", "full_name", "member_name",
    "bank_account", "credit_card",
}

# ── Max rows to prevent unbounded queries ──
MAX_RESULT_ROWS = 10_000
QUERY_ROW_LIMIT = 5_000


def validate_sql(sql: str) -> tuple[bool, str | None]:
    """Check SQL for dangerous operations. Returns (is_safe, reason)."""
    upper = sql.upper()

    for pattern in BLOCKED_SQL_PATTERNS:
        if re.search(pattern, upper):
            reason = f"Blocked SQL pattern detected: {pattern}"
            logger.warning("sql_blocked", reason=reason, sql_preview=sql[:200])
            return False, reason

    return True, None


def enforce_row_limit(sql: str) -> str:
    """Append LIMIT clause if not already present."""
    upper = sql.strip().upper()
    if "LIMIT" not in upper:
        sql = sql.rstrip().rstrip(";")
        sql += f"\nLIMIT {QUERY_ROW_LIMIT}"
    return sql


def mask_pii_columns(data: dict[str, Any]) -> dict[str, Any]:
    """Replace PII column values with masked placeholders."""
    masked = {}
    for key, value in data.items():
        if key.lower() in PII_COLUMNS:
            masked[key] = "***REDACTED***"
        else:
            masked[key] = value
    return masked


def mask_pii_dataframe(df) -> Any:
    """Mask PII columns in a pandas DataFrame."""
    import pandas as pd

    for col in df.columns:
        if col.lower() in PII_COLUMNS:
            df[col] = "***REDACTED***"
    return df


def validate_output(text: str) -> str:
    """Scan agent output text for potential PII leakage."""
    # Simple SSN pattern
    text = re.sub(r"\b\d{3}-\d{2}-\d{4}\b", "***-**-****", text)
    # Phone patterns
    text = re.sub(r"\b\d{3}[-.)]\s*\d{3}[-.)]\s*\d{4}\b", "***-***-****", text)
    return text
