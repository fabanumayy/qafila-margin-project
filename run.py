"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg."""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"


def get_catalog(warehouse: Path) -> SqlCatalog:
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        "qafila",
        uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}",
        warehouse=warehouse.resolve().as_posix(),
    )
    for ns in ("staging", "silver", "gold", "quality"):
        try:
            catalog.create_namespace(ns)
        except Exception:
            pass
    return catalog


def _load_sqlite_table(con: duckdb.DuckDBPyConnection, sqlite_path: Path, name: str) -> None:
    with sqlite3.connect(sqlite_path) as src:
        frame = pd.read_sql_query(f"SELECT * FROM {name}", src)
    con.register(f"_p_{name}", frame)
    con.sql(f"CREATE OR REPLACE TABLE raw.{name} AS SELECT * FROM _p_{name}")
    con.unregister(f"_p_{name}")


def ingest(data_dir: Path, db: Path) -> None:
    """Land all source tables and every supplier row, replacing the raw landing atomically."""
    sqlite_path = data_dir / "qafila_oltp.sqlite"
    files = sorted((data_dir / "suppliers").glob("prices_*.csv"))
    with duckdb.connect(str(db)) as con:
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")
        for table in ("orders", "order_items", "products", "stores"):
            _load_sqlite_table(con, sqlite_path, table)

        rows: list[dict[str, str | None]] = []
        for path in files:
            with path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    rows.append({**row, "source_file": path.name})
        if rows:
            # All supplier fields stay as strings: this preserves malformed values exactly.
            cols = sorted({key for row in rows for key in row})
            frame = pd.DataFrame(rows, columns=cols).where(pd.notna, None)
        else:
            frame = pd.DataFrame(columns=["source_file", "sku", "valid_from"])
        con.register("_supplier_rows", frame)
        con.sql("CREATE OR REPLACE TABLE raw.supplier_prices AS SELECT * FROM _supplier_rows")
        con.unregister("_supplier_rows")


def transform(db: Path) -> None:
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()", "build",
           "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def publish(db: Path, warehouse: Path) -> None:
    catalog = get_catalog(warehouse)
    mappings = {
        "silver.product_cost_history": "silver.product_cost_history",
        "gold.weekly_margin": "gold.weekly_margin",
        "quality.price_quarantine": "quality.price_quarantine",
    }
    with duckdb.connect(str(db), read_only=True) as con:
        for identifier, relation in mappings.items():
            frame = con.sql(f"SELECT * FROM {relation}").fetchdf()
            # Arrow preserves DATE/BOOLEAN/numeric types expected by the contract.
            arrow = pa.Table.from_pandas(frame, preserve_index=False)
            try:
                table = catalog.load_table(identifier)
                table.overwrite(arrow, snapshot_properties={"qafila.pipeline": "run"})
            except Exception:
                catalog.create_table(identifier, schema=pa.Schema.from_pandas(frame), location=None)
                catalog.load_table(identifier).append(arrow, snapshot_properties={"qafila.pipeline": "run"})


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()
    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
