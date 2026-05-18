"""DuckDB-backed warehouse for persistent daily data accumulation.

Each silver parquet file maps to one DuckDB table. On append, only rows
with a date not already present are inserted (date-prefixed composite keys).
Metadata tracked in _warehouse_meta table.

Usage:
    from finsynapse.warehouse.store import Warehouse
    wh = Warehouse()
    wh.append_all()           # append today's silver data
    wh.append_file(path)      # append a single parquet
    wh.status()               # row counts per table
    wh.query("SELECT ...")    # arbitrary SQL
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from finsynapse import config as _cfg

# Table definitions: (table_name, silver_parquet_filename, composite_key_columns)
# health_log has no natural key — append-only with dedup on all columns.
_REGISTRY: list[dict] = [
    {
        "table": "macro_daily",
        "file": "macro_daily.parquet",
        "keys": ["date", "indicator"],
        "date_col": "date",
    },
    {
        "table": "percentile_daily",
        "file": "percentile_daily.parquet",
        "keys": ["date", "indicator"],
        "date_col": "date",
    },
    {
        "table": "temperature_daily",
        "file": "temperature_daily.parquet",
        "keys": ["date", "market"],
        "date_col": "date",
    },
    {
        "table": "divergence_daily",
        "file": "divergence_daily.parquet",
        "keys": ["date", "pair_name"],
        "date_col": "date",
    },
    {
        "table": "health_log",
        "file": "health_log.parquet",
        "keys": ["date", "indicator", "rule", "severity"],
        "date_col": "date",
    },
]

DEFAULT_DB_PATH = _cfg.settings.data_dir / "warehouse" / "finsynapse.duckdb"


class Warehouse:
    """Persistent DuckDB warehouse for daily silver data accumulation."""

    def __init__(self, db_path: Path | str | None = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: duckdb.DuckDBPyConnection | None = None

    # ------------------------------------------------------------------
    # connection management
    # ------------------------------------------------------------------

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self._conn = duckdb.connect(str(self.db_path))
            self._ensure_meta()
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _ensure_meta(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _warehouse_meta (
                table_name VARCHAR PRIMARY KEY,
                row_count BIGINT,
                min_date DATE,
                max_date DATE,
                last_append_utc TIMESTAMP,
                source_file VARCHAR
            )
            """
        )

    # ------------------------------------------------------------------
    # core append logic
    # ------------------------------------------------------------------

    def append_file(
        self,
        parquet_path: Path | str,
        table_name: str | None = None,
    ) -> dict:
        """Append one silver parquet file to the warehouse.

        Returns a dict with counts for reporting.
        """
        parquet_path = Path(parquet_path)
        if not parquet_path.exists():
            return {"table": table_name or parquet_path.stem, "status": "skipped", "reason": "file_not_found"}

        # resolve table entry
        entry = None
        for e in _REGISTRY:
            if e["file"] == parquet_path.name or e["table"] == (table_name or ""):
                entry = e
                break

        if entry is None:
            # ad-hoc table: use filename stem as table name, date as single key
            name = table_name or parquet_path.stem
            entry = {
                "table": name,
                "file": parquet_path.name,
                "keys": ["date"],
                "date_col": "date",
            }

        tbl = entry["table"]
        keys = entry["keys"]

        df = pd.read_parquet(parquet_path)
        if df.empty:
            return {"table": tbl, "status": "skipped", "reason": "empty_parquet"}

        # normalise date column to datetime (some parquets store as object/string)
        date_col = entry["date_col"]
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col])

        existing_count = self._count_table(tbl)

        if existing_count == 0:
            # first load — create table and insert everything
            self.conn.execute(f'DROP TABLE IF EXISTS "{tbl}"')
            self.conn.execute(f'CREATE TABLE "{tbl}" AS SELECT * FROM df')
            new_count = len(df)
        else:
            # diff-based append: only insert rows whose key combo is not in warehouse
            new_count = self._upsert(df, tbl, keys)

        # update meta
        min_d = df[date_col].min() if date_col in df.columns else None
        max_d = df[date_col].max() if date_col in df.columns else None
        total = self._count_table(tbl)
        self._update_meta(tbl, total, min_d, max_d, parquet_path.name)

        return {
            "table": tbl,
            "status": "appended",
            "new_rows": new_count,
            "total_rows": total,
            "min_date": str(min_d.date()) if min_d is not None else None,
            "max_date": str(max_d.date()) if max_d is not None else None,
        }

    def append_all(self, silver_dir: Path | str | None = None) -> list[dict]:
        """Append all silver parquet files from the silver directory."""
        silver_dir = Path(silver_dir) if silver_dir else _cfg.settings.silver_dir
        if not silver_dir.exists():
            return [{"table": "*", "status": "error", "reason": f"dir_not_found: {silver_dir}"}]

        results = []
        for entry in _REGISTRY:
            path = silver_dir / entry["file"]
            if path.exists():
                results.append(self.append_file(path, entry["table"]))
            else:
                results.append({"table": entry["table"], "status": "skipped", "reason": "file_not_found"})
        return results

    def status(self) -> pd.DataFrame:
        """Return a DataFrame summarising every tracked table."""
        try:
            return self.conn.execute("SELECT * FROM _warehouse_meta ORDER BY table_name").fetchdf()
        except Exception:
            return pd.DataFrame(
                columns=["table_name", "row_count", "min_date", "max_date", "last_append_utc", "source_file"]
            )

    def query(self, sql: str) -> pd.DataFrame:
        """Run an arbitrary SQL query against the warehouse and return a DataFrame."""
        return self.conn.execute(sql).fetchdf()

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _count_table(self, table: str) -> int:
        try:
            row = self.conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def _upsert(self, df: pd.DataFrame, table: str, keys: list[str]) -> int:
        """Insert rows whose key is not already present. Returns count of new rows."""
        # build anti-join: select incoming keys not in warehouse
        key_cols = [k for k in keys if k in df.columns]
        if not key_cols:
            # fallback: insert all, let DuckDB handle if we add PK constraint
            self.conn.execute(f'INSERT INTO "{table}" SELECT * FROM df')
            return len(df)

        # register incoming as temp view
        self.conn.register("_incoming", df)

        # build WHERE clause for anti-join
        where_clauses = []
        for k in key_cols:
            where_clauses.append(f'w."{k}" IS NULL')

        upsert_sql = f"""
            INSERT INTO "{table}"
            SELECT i.* FROM _incoming i
            LEFT JOIN "{table}" w ON {" AND ".join(f'i."{k}" = w."{k}"' for k in key_cols)}
            WHERE {" AND ".join(where_clauses)}
        """
        before = self._count_table(table)
        self.conn.execute(upsert_sql)
        self.conn.unregister("_incoming")
        after = self._count_table(table)
        return after - before

    def _update_meta(
        self,
        table: str,
        row_count: int,
        min_date: pd.Timestamp | None,
        max_date: pd.Timestamp | None,
        source_file: str,
    ) -> None:
        min_s = min_date.strftime("%Y-%m-%d") if min_date is not None else None
        max_s = max_date.strftime("%Y-%m-%d") if max_date is not None else None
        now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

        self.conn.execute(
            """
            INSERT OR REPLACE INTO _warehouse_meta
                (table_name, row_count, min_date, max_date, last_append_utc, source_file)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (table, row_count, min_s, max_s, now, source_file),
        )

    def rebuild_all(self, silver_dir: Path | str | None = None) -> list[dict]:
        """Full rebuild: drop all tables and re-insert from silver.

        Use this after a schema change or when the silver layer has been
        rebuilt with updated upstream data.
        """
        silver_dir = Path(silver_dir) if silver_dir else _cfg.settings.silver_dir
        # drop all tracked tables
        for entry in _REGISTRY:
            self.conn.execute(f'DROP TABLE IF EXISTS "{entry["table"]}"')
        self.conn.execute("DELETE FROM _warehouse_meta")
        return self.append_all(silver_dir)
