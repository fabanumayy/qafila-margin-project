# Qafila gross margin

Submitted by: `fabanumayy` — academy: @SDAIAAcademy

## Design

The pipeline is: supplier files and SQLite → `raw` DuckDB landing → dbt staging views → dbt `silver`/`gold`/`quality` tables → in-place Iceberg publication. `run.py` reloads the raw landing on every run, so it is deterministic and does not duplicate source rows.

| Model | Layer | Grain | Materialization |
|---|---|---|---|
| `stg_supplier_prices` | staging | one row per supplier-file row | view |
| `stg_order_lines` | staging | one row per order line | view |
| `product_cost_history` | silver | one row per SKU and changed cost version | table |
| `weekly_margin` | gold | one row per ISO week, city and category | table |
| `price_quarantine` | quality | one row per rejected supplier row | table |

The SCD2 model first trims SKUs and parses both cost column names, decimal commas and Arabic-Indic digits. Valid duplicate `(sku, valid_from)` rows are resolved by the lexicographically last file name, so the week-39 correction wins. A new history version is created only when the rounded cost changes; missing weekly quotes therefore keep the previous version, and `valid_to` is exclusive.

Each eligible delivered order line is converted from UTC to Riyadh date before joining to the cost version with `valid_from <= order_date < valid_to`. Uncosted lines remain in revenue and line counts, but are excluded from cost and counted in `uncosted_lines`.

## Data problems handled

The generated course data contains four supplier files with 160 rows each, plus the optional correction file. The loader handles the UTF-8 BOM, changing headers (`unit_cost` versus `cost_sar`), quoted decimal commas, Arabic-Indic digits, whitespace around SKUs, zero and negative costs, and SKUs absent from Qafila's product master. On the seed-2026 dataset after the correction, the quality table contains 7 rows: 4 `invalid_cost` rows and 3 `unknown_sku` rows. The exact raw cost text is retained in `raw_cost`.

## Tests

The dbt build runs 11 tests, including not-null checks for key fields, positive costs and line counts, and accepted quarantine reasons. `tools/check.py` also verifies SCD2 continuity, one current row per SKU, point-in-time margin consistency, eligible-line coverage, and Iceberg snapshots.

## Re-runs and the correction

Running `run.py` repeatedly rebuilds the deterministic DuckDB models and overwrites the existing Iceberg tables in place, creating a new snapshot without dropping the table. On the course dataset, the initial run produced 633 history rows, 165 margin rows and 7 quarantine rows; after `prices_2026-W39_fix.csv` arrived, history grew to 637 rows while the margin table remained 165 rows with corrected costs. The catalog retained the earlier `gold.weekly_margin` snapshots, so a prior snapshot can still be read with PyIceberg time travel.

The implementation was tested from an empty warehouse on both the default generated dataset and `--seed 11`; `tools/check.py` passed 22/22 checks in both cases. No data, DuckDB warehouse, or generated files are committed.

## What I'd do next

At 100× the current volume, I would partition raw and Iceberg data by supplier week and order week, use incremental dbt models keyed by source file and business grain, and add manifest-based ingestion so unchanged files are not reparsed. Daily supplier files would use the same SCD2 logic with a file-arrival audit table and explicit late-arrival replay window.

## AI assistance

AI assistance was used to implement and test the pipeline. The repository code was reviewed against the project contract, tested on two generated seeds, and checked with the supplied validation script.
