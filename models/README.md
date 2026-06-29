# dbt models guide

silver/gold medallion models built on the bronze `bronze.metrics` Iceberg table.
All models are **Iceberg** tables using the **incremental `merge`** strategy, partitioned by
`dt`, and bounded each run by a `lookback_days` window (project var, default 3) so steady-state
runs only scan/merge recent partitions. Run everything with `uv run dbt build`.

## Layer / DAG

```
source: bronze.metrics            (read-only contract; declared in models/bronze/)
  └─ silver.silver_metrics        dedup + typed (1 row per natural key)
       ├─ gold.gold_metrics_daily        daily avg/min/max/count per device+parameter
       │    └─ gold.gold_metrics_daily_aa010   (measurement='AA010' slice)
       └─ silver.silver_metrics_aa010          (measurement='AA010' slice)
```

Output Glue databases come from each folder's `+schema` in `dbt_project.yml`:
`silver/` → `silver`, `gold/` → `gold`. The measurement-scoped example models live in
the `silver/` and `gold/` folders alongside their parents, so they land in the same schemas.

## Why `dt` (not `day(ts)`)

`dt` is bronze's physical Iceberg partition column (identity partition) = the UTC date of `ts`,
computed once at ingestion. Filtering `where dt >= …` prunes partitions at the metadata level
(cheap). `day(ts)`/`date(ts)` is a per-row expression on a non-partition `timestamptz` column —
it can't prune partitions (risking a full scan) and re-derives the date (timezone risk) every
query. So every incremental filter here uses `dt`.

## Models

### `bronze.metrics` (source)
Declared in `models/bronze/_bronze__source.yml`. Append-only event metrics produced by
`../s3-pipeline`. Columns: `cdevice, pdevice, parameter` (tags), `value` (double field),
`ts` (timestamptz μs UTC), `ts_ns` (bigint, lossless), `measurement`, `dt` (date partition).
Read-only — never built here.

### `silver.silver_metrics`
`models/silver/silver_metrics.sql`. Deduplicates the append-only bronze and selects the typed
columns. Natural key = `(measurement, cdevice, pdevice, parameter, ts_ns)`; within each run a
`row_number()` keeps one row per key, then MERGE upserts on that key. Tests: `not_null` on
`value/ts/ts_ns`, unique-combination on the natural key.

### `gold.gold_metrics_daily`
`models/gold/gold_metrics_daily.sql`. Daily aggregate from `silver_metrics`, grain
`(measurement, cdevice, pdevice, parameter, dt)`: `avg/min/max(value)` + `sample_count`.
Tests: unique-combination on the grain, `sample_count >= 1`.

### Example: a single-measurement slice (`AA010`)
Shows how to carve one measurement into its own marts by filtering the shared layers
(DRY — reuses upstream dedup/aggregate rather than re-deriving). Each example model lives
in its own layer folder, so it inherits that layer's schema:

- `silver.silver_metrics_aa010` (`models/silver/`) ← `ref('silver_metrics')` `where measurement = 'AA010'`
- `gold.gold_metrics_daily_aa010` (`models/gold/`) ← `ref('gold_metrics_daily')` `where measurement = 'AA010'`

Both keep the same keys/grain and Iceberg incremental-merge pattern as their parents, so they
build and test exactly like the core models. To slice a different measurement, copy a model
and change the `where measurement = '…'` literal (and the file/model name).

## Live verification

Running these against real Athena (`dbt debug`, `dbt build`, dedup/aggregate spot-checks) is
covered in [../deploy/README.md](../deploy/README.md) §1.
