"""Persistent DuckDB warehouse for accumulating daily silver-layer data.

The warehouse is the answer to "run this project for 10 years and keep all
the data": while bronze/silver are gitignored and rebuilt from scratch each
CI run, the warehouse persists every daily snapshot so the full history is
queryable locally and survives across runs.

Tables mirror the silver parquet schema with date-prefixed primary keys.
Append is idempotent: duplicate keys are skipped (INSERT OR IGNORE).
"""
