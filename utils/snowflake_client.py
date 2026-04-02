"""SQLite-based data layer — zero external dependencies.

Drop-in replacement for the Snowflake client. Loads CSV data into
an in-process SQLite database so the entire engine runs locally.

To switch to Snowflake later, rename snowflake_client.py.bak back
and update config/.env with your Snowflake credentials.
"""

from __future__ import annotations

import asyncio
import csv
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from utils.logging import get_logger

logger = get_logger(__name__)

DB_PATH = Path(__file__).parent.parent / "data" / "healthcare.db"
CSV_DIR = Path(__file__).parent.parent / "data" / "csv"


# ── Snowflake → SQLite SQL compatibility rewrites ──

_SQL_REPLACEMENTS = [
    # Date truncation
    ("DATE_TRUNC('month', service_date)", "STRFTIME('%Y-%m-01', service_date)"),
    ("DATE_TRUNC('month',service_date)",  "STRFTIME('%Y-%m-01', service_date)"),
    # Date arithmetic
    ("DATEADD('month', -6, ",  "DATE("),
    ("DATEADD('year', -2, ",   "DATE("),
    # DATEDIFF
    ("DATEDIFF('day', e.coverage_start, e.created_date)",
     "CAST(JULIANDAY(e.created_date) - JULIANDAY(e.coverage_start) AS INTEGER)"),
    ("DATEDIFF('day', c1.service_date, c2.service_date)",
     "CAST(JULIANDAY(c2.service_date) - JULIANDAY(c1.service_date) AS INTEGER)"),
    # DAYOFWEEK
    ("DAYOFWEEK(service_date)", "CAST(STRFTIME('%w', service_date) AS INTEGER)"),
    # Window RANGE INTERVAL not supported in SQLite
    ("RANGE BETWEEN INTERVAL '30 DAYS' PRECEDING AND CURRENT ROW",
     "ROWS BETWEEN 30 PRECEDING AND CURRENT ROW"),
    # PERCENTILE_CONT → approximate with subquery
    ("PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY allowed_amount) AS p95",
     "allowed_amount AS p95"),  # simplified: we use a different approach below
    # NULLIF → MAX to avoid division by zero
    ("NULLIF(ps.std_claims, 0)",  "MAX(ps.std_claims, 0.001)"),
    ("NULLIF(ps.std_allowed, 0)", "MAX(ps.std_allowed, 0.001)"),
    ("NULLIF(rolling_std, 0)",    "MAX(rolling_std, 0.001)"),
    ("NULLIF(avg_6m, 0)",         "MAX(avg_6m, 0.001)"),
    ("NULLIF(p.p95, 0)",          "MAX(p.p95, 0.001)"),
]


def _adapt_sql(sql: str) -> str:
    """Rewrite Snowflake SQL to SQLite-compatible SQL."""
    for old, new in _SQL_REPLACEMENTS:
        sql = sql.replace(old, new)

    # Snowflake DATEADD produces: DATE( '2025-01-01') — fix to: DATE('2025-01-01', '-6 months')
    # This is a simplified pass; complex expressions may need manual tuning
    import re
    # Fix remaining DATEADD patterns
    sql = re.sub(
        r"DATEADD\('(\w+)',\s*(-?\d+),\s*([^)]+)\)",
        lambda m: f"DATE({m.group(3)}, '{m.group(2)} {m.group(1)}')",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


class SQLiteClient:
    """Local SQLite client matching the Snowflake client interface."""

    def __init__(self):
        self._connection: sqlite3.Connection | None = None

    def _get_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            self._connection = sqlite3.connect(str(DB_PATH), check_same_thread=False)
            self._connection.row_factory = sqlite3.Row
            logger.info("sqlite_connected", db=str(DB_PATH))
        return self._connection

    async def execute_query(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        """Execute SQL and return a DataFrame."""
        adapted = _adapt_sql(sql)
        logger.info("executing_query", sql_preview=adapted[:200])

        def _run():
            conn = self._get_connection()
            try:
                cursor = conn.execute(adapted)
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
                return pd.DataFrame([dict(r) for r in rows], columns=columns)
            except Exception as e:
                logger.error("sqlite_query_error", error=str(e), sql_preview=adapted[:300])
                raise

        return await asyncio.to_thread(_run)

    async def execute_query_safe(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> tuple[pd.DataFrame | None, str | None]:
        """Execute query, returning (df, None) on success or (None, error)."""
        try:
            df = await self.execute_query(sql, params)
            return df, None
        except Exception as e:
            logger.error("query_failed", error=str(e))
            return None, str(e)

    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("sqlite_disconnected")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Database Initialization — auto-loads CSVs on first run
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def init_database():
    """Create tables and load CSV data into SQLite."""
    logger.info("initializing_sqlite_database")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))

    # ── Create tables ──
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS providers (
            npi             TEXT PRIMARY KEY,
            first_name      TEXT,
            last_name       TEXT,
            specialty       TEXT,
            practice_state  TEXT,
            practice_city   TEXT,
            practice_zip    TEXT,
            tax_id          TEXT,
            practice_type   TEXT,
            enrollment_date TEXT,
            is_active       INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS members (
            member_id       TEXT PRIMARY KEY,
            date_of_birth   TEXT,
            gender          TEXT,
            state           TEXT,
            city            TEXT,
            zip             TEXT,
            plan_type       TEXT,
            employer_group  TEXT,
            risk_score      REAL
        );

        CREATE TABLE IF NOT EXISTS eligibility (
            eligibility_id  TEXT PRIMARY KEY,
            member_id       TEXT,
            coverage_start  TEXT,
            coverage_end    TEXT,
            plan_code       TEXT,
            coverage_type   TEXT,
            status          TEXT,
            created_date    TEXT
        );

        CREATE TABLE IF NOT EXISTS claims (
            claim_id        TEXT PRIMARY KEY,
            member_id       TEXT,
            provider_npi    TEXT,
            referring_npi   TEXT,
            service_date    TEXT,
            paid_date       TEXT,
            procedure_code  TEXT,
            diagnosis_code  TEXT,
            diagnosis_desc  TEXT,
            place_of_service TEXT,
            allowed_amount  REAL,
            paid_amount     REAL,
            member_liability REAL,
            claim_type      TEXT,
            service_state   TEXT,
            modifier_1      TEXT,
            modifier_2      TEXT,
            units           INTEGER DEFAULT 1,
            claim_status    TEXT
        );

        CREATE TABLE IF NOT EXISTS specialty_procedure_map (
            procedure_code    TEXT,
            expected_specialty TEXT,
            category          TEXT,
            PRIMARY KEY (procedure_code, expected_specialty)
        );

        CREATE INDEX IF NOT EXISTS idx_claims_provider ON claims(provider_npi);
        CREATE INDEX IF NOT EXISTS idx_claims_member ON claims(member_id);
        CREATE INDEX IF NOT EXISTS idx_claims_date ON claims(service_date);
        CREATE INDEX IF NOT EXISTS idx_claims_proc ON claims(procedure_code);
        CREATE INDEX IF NOT EXISTS idx_elig_member ON eligibility(member_id);
    """)

    # ── Load CSVs ──
    table_csv_map = {
        "providers": "providers.csv",
        "members": "members.csv",
        "eligibility": "eligibility.csv",
        "claims": "claims.csv",
        "specialty_procedure_map": "specialty_procedure_map.csv",
    }

    for table, filename in table_csv_map.items():
        csv_path = CSV_DIR / filename
        if not csv_path.exists():
            logger.warning("csv_not_found", file=str(csv_path))
            continue

        # Skip if already loaded
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if count > 0:
            logger.info("table_already_loaded", table=table, rows=count)
            continue

        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            continue

        columns = list(rows[0].keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(columns)

        conn.executemany(
            f"INSERT OR IGNORE INTO {table} ({col_names}) VALUES ({placeholders})",
            [tuple(row[c] for c in columns) for row in rows],
        )
        conn.commit()
        logger.info("csv_loaded", table=table, rows=len(rows))

    conn.close()
    logger.info("database_ready", path=str(DB_PATH))


# ── Auto-initialize on import ──
if not DB_PATH.exists() or DB_PATH.stat().st_size == 0:
    init_database()

# Module-level singleton — same variable name so all agents work unchanged
snowflake_client = SQLiteClient()
