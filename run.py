"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3   
import subprocess
import sys
from pathlib import Path

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Load app tables and supplier CSV files into DuckDB schema raw.

    Re-running ingest replaces the raw tables instead of duplicating rows.
    """
    sqlite_path = data_dir / "qafila_oltp.sqlite"
    supplier_dir = data_dir / "suppliers"

    if not sqlite_path.is_file():
        raise FileNotFoundError(f"App database not found: {sqlite_path}")

    if not supplier_dir.is_dir():
        raise FileNotFoundError(f"Supplier directory not found: {supplier_dir}")

    supplier_files = sorted(supplier_dir.glob("prices_*.csv"))
    if not supplier_files:
        raise FileNotFoundError(
            f"No supplier price files found in {supplier_dir}"
        )

    app_tables = ("orders", "order_items", "products", "stores")

    # Read the SQLite source using Python's built-in sqlite3 module.
    with sqlite3.connect(str(sqlite_path)) as source:
        source_tables = {
            row[0]
            for row in source.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

        missing = set(app_tables) - source_tables
        if missing:
            raise ValueError(
                f"Missing app tables in {sqlite_path}: {sorted(missing)}"
            )

        app_data = {}

        for table in app_tables:
            info = source.execute(
                f'PRAGMA table_info("{table}")'
            ).fetchall()

            columns = [row[1] for row in info]
            if not columns:
                raise ValueError(f"Table {table} has no columns")

            rows = source.execute(
                f'SELECT * FROM "{table}"'
            ).fetchall()

            # Keep the original column names and SQLite values.
            # Translate SQLite column types into DuckDB-compatible types.
            definitions = []
            for row in info:
                name = row[1]
                declared_type = (row[2] or "").upper()

                if any(
                    token in declared_type
                    for token in ("INT",)
                ):
                    sql_type = "BIGINT"
                elif any(
                    token in declared_type
                    for token in (
                        "REAL", "FLOA", "DOUB", "NUMERIC", "DECIMAL"
                    )
                ):
                    sql_type = "DOUBLE"
                elif "BLOB" in declared_type:
                    sql_type = "BLOB"
                else:
                    sql_type = "VARCHAR"

                safe_name = '"' + name.replace('"', '""') + '"'
                definitions.append(f"{safe_name} {sql_type}")

            app_data[table] = (columns, definitions, rows)

    # Read all supplier files as text. This preserves raw prices such as
    # decimal commas and Arabic-Indic digits for the cleaning stage.
    supplier_columns = []
    supplier_rows = []

    for path in supplier_files:
        with path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as handle:
            reader = csv.DictReader(handle)

            if not reader.fieldnames:
                raise ValueError(f"CSV has no header: {path.name}")

            if "source_file" in reader.fieldnames:
                raise ValueError(
                    f"Reserved column source_file in {path.name}"
                )

            for column in reader.fieldnames:
                if column not in supplier_columns:
                    supplier_columns.append(column)

            for row in reader:
                row["source_file"] = path.name
                supplier_rows.append(row)

    supplier_columns.append("source_file")

    # Build raw tables. Each run replaces the previous raw data,
    # so it cannot append duplicate rows.
    with duckdb.connect(str(db)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        for table, (columns, definitions, rows) in app_data.items():
            con.execute(f'DROP TABLE IF EXISTS raw."{table}"')
            con.execute(
                f'CREATE TABLE raw."{table}" '
                f'({", ".join(definitions)})'
            )

            if rows:
                quoted_columns = ", ".join(
                    '"' + name.replace('"', '""') + '"'
                    for name in columns
                )
                placeholders = ", ".join("?" for _ in columns)

                con.executemany(
                    f'INSERT INTO raw."{table}" '
                    f'({quoted_columns}) VALUES ({placeholders})',
                    rows,
                )

        con.execute(
            'DROP TABLE IF EXISTS raw."supplier_prices"'
        )

        supplier_definitions = ", ".join(
            '"' + name.replace('"', '""') + '" VARCHAR'
            for name in supplier_columns
        )

        con.execute(
            'CREATE TABLE raw."supplier_prices" '
            f'({supplier_definitions})'
        )

        if supplier_rows:
            quoted_columns = ", ".join(
                '"' + name.replace('"', '""') + '"'
                for name in supplier_columns
            )
            placeholders = ", ".join("?" for _ in supplier_columns)

            values = [
                tuple(row.get(column) for column in supplier_columns)
                for row in supplier_rows
            ]

            con.executemany(
                'INSERT INTO raw."supplier_prices" '
                f'({quoted_columns}) VALUES ({placeholders})',
                values,
            )

        # Report what was loaded for a quick sanity check.
        for table in (*app_tables, "supplier_prices"):
            count = con.execute(
                f'SELECT COUNT(*) FROM raw."{table}"'
            ).fetchone()[0]
            print(f"raw.{table}: {count} rows")
