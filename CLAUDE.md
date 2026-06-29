# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

The **silver / gold** layer of a medallion lakehouse, built with **dbt + Athena** on
top of a pre-existing **bronze Iceberg table**. It is the downstream continuation of
the sibling project `../s3-pipeline` (raw → bronze). End-to-end flow:

```
MSK (Kafka) → Firehose → S3 raw → S3 bronze (Iceberg)     ← ../s3-pipeline (DONE)
                                     → dbt-athena → silver → gold   ← THIS REPO
```

This repo does **not** ingest or write bronze. It **reads** the bronze table and
materializes silver/gold tables via Athena (Iceberg). dbt is run as a **periodic ECS
Fargate batch task** — the same containerized one-shot pattern as `../s3-pipeline`.
The goal is a **PoC against real AWS Athena**, not a local-only mock.

## The bronze source contract (upstream input — do not redefine)

dbt's source is the Glue/Iceberg table `bronze.metrics`, produced by `../s3-pipeline`.
Treat its schema as a fixed contract — declare it as a dbt `source`, never as a model:

| column        | type                         | meaning                                    |
|---------------|------------------------------|--------------------------------------------|
| `cdevice`     | string                       | tag (device)                               |
| `pdevice`     | string                       | tag (device)                               |
| `parameter`   | string                       | metric name                                |
| `value`       | double                       | metric value                               |
| `ts`          | `timestamp(6) with time zone`| event time, μs precision, UTC              |
| `ts_ns`       | bigint                       | original event time in ns (lossless)       |
| `measurement` | string                       | line-protocol measurement                  |
| `dt`          | date                         | event-date partition key (UTC date of `ts`)|

Partitioning is on `dt`. Prefer `dt` predicates in incremental filters to prune
partitions and keep Athena scan cost (and PoC bill) down.

## Layering intent

- **silver**: cleaned/typed/deduped/conformed from bronze — one row per logical event,
  standardized columns, late/invalid data handled. Incremental on `dt`/`ts`.
- **gold**: business-facing aggregates (e.g. per-device / per-parameter rollups over
  time windows) consumed by downstream analytics.

Silver/gold model names are not yet fixed — derive them from the bronze grain
(`measurement` + device tags + `parameter` + time). Do not invent metrics that the
bronze columns can't support.

## Repository conventions

These are project rules, not suggestions (carried over from `../s3-pipeline`):

- **Package management is `uv`.** Add deps with `uv add <pkg>`, run with `uv run <cmd>`.
  No bare `pip`, no hand-editing pins. dbt itself runs under uv: `uv run dbt ...`.
- **Real Athena for the PoC.** Validate models against actual Athena, not only local
  parsing. Be mindful of bytes-scanned cost — always filter on the `dt` partition.
- **Athena Iceberg outputs.** Materialize silver/gold as Iceberg tables (`table_type =
  'iceberg'`) so they support incremental MERGE and schema evolution.
- **Secrets / account-specific config stay out of git** — credentials, S3 paths, and
  workgroup live in env/`profiles.yml` (gitignored), mirroring `../s3-pipeline`'s
  `deploy/config.env` + `deploy/task.env` split.

## Getting started (no scaffolding exists yet)

The project is not yet initialized — there is no `pyproject.toml`, dbt project, or
`profiles.yml`. When creating it, establish and then **update this file** with the real
model names, profile/target names, and deploy commands.

```bash
uv init                                  # create pyproject.toml
uv add dbt-core dbt-athena-community     # dbt + Athena adapter
uv run dbt init <project_name>           # scaffold dbt project (models/, dbt_project.yml)
```

dbt-athena `profiles.yml` essentials (Athena needs an S3 staging dir + workgroup +
the bronze database; the source database is `bronze`, the output schema is silver/gold):

```yaml
# ~/.dbt/profiles.yml (or DBT_PROFILES_DIR) — gitignored, account-specific
<project_name>:
  target: dev
  outputs:
    dev:
      type: athena
      region_name: <aws-region>
      database: awsdatacatalog        # Glue catalog
      schema: silver                  # output schema for built models
      s3_staging_dir: s3://<bucket>/_athena/staging/
      s3_data_dir:    s3://<bucket>/silver/   # where Iceberg table data is written
      work_group: <athena-workgroup>
      threads: 4
```

## Commands (establish these as the project is built, then update this file)

```bash
# Build everything (run models + run tests)
uv run dbt build

# Run/test a single model (silver/gold dev loop)
uv run dbt run  --select <model_name>
uv run dbt test --select <model_name>

# Build a model and everything downstream of it
uv run dbt build --select <model_name>+

# Validate connection / source freshness against real Athena
uv run dbt debug
uv run dbt source freshness

# Compile to inspect generated SQL without running
uv run dbt compile --select <model_name>
```

- **Lint/format**: not yet configured (consider `sqlfluff` with the athena dialect).
- **ECS deploy**: not yet configured. Mirror `../s3-pipeline/deploy/` — Dockerfile on
  the `ghcr.io/astral-sh/uv` base running `uv run dbt build`, a Fargate task definition
  reading runtime env from an S3 env file, and an EventBridge Scheduler for periodic
  runs. Capture the build/push/register/schedule commands here once they exist.

## Cross-repo note

When the bronze schema or partitioning in `../s3-pipeline` changes (see its
`src/raw_to_bronze/transform.py` `BRONZE_COLUMNS` and the `dt` derivation), the dbt
`source` definition here must change in lockstep — they are the same contract.
