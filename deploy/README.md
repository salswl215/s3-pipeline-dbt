# Deploy & AWS verification runbook

This runbook covers every step that needs **real AWS credentials / live Athena** — the parts
that were intentionally deferred while the dbt code was built and statically validated. Run
these once your AWS access is ready.

Project entrypoint: the container runs `dbt build --target prod` (silver → gold + all tests).

---

## 0. Prerequisites

- AWS credentials in your shell (e.g. `aws sts get-caller-identity` works) for the target account/region.
- `bronze.metrics` Iceberg table exists and has data (already live).
- An Athena **workgroup** with query results enabled, and two S3 locations:
  - a **staging dir** (Athena query results), e.g. `s3://<bucket>/_athena/staging/`
  - a **data dir** for silver/gold table data, e.g. `s3://<bucket>/silver/` (gold writes under `…/gold/` via its `+schema`)
- Tools: `uv`, `docker`, `aws` CLI, `envsubst` (from gettext), `python3`.
- Existing ECS cluster + VPC subnets + security group (the task runs on Fargate, `awsvpc`).

> **Cost note (PoC):** every model run scans Athena. silver/gold are incremental MERGE bounded by
> `lookback_days` (default 3) so steady-state scans are small. **The first `dbt build` scans all of
> bronze once** (seeding silver). If bronze is large, set a floor — see §A.

---

## Part 1 — Local verification against real Athena

These are the deferred verification steps from plan Tasks 2–6. Run from the repo root.

### 1.1 Export connection env

```bash
export AWS_REGION=<region>
export ATHENA_STAGING_DIR=s3://<bucket>/_athena/staging/
export ATHENA_DATA_DIR=s3://<bucket>/silver/
export ATHENA_WORKGROUP=<workgroup>
export DBT_SCHEMA=silver        # default output schema; gold models override to `gold`
export DBT_TARGET=dev
```

### 1.2 Install packages + test the connection (Task 2 Step 3)

```bash
uv run dbt deps
uv run dbt debug
```
Expected: `Connection test: OK` — Athena reachable, catalog + staging accessible.

### 1.3 Confirm the bronze source resolves (Task 3 Step 2)

```bash
uv run dbt source freshness
```
Expected: a real Athena query against `bronze.metrics` returns a freshness state (PASS/WARN) — proves the source is wired and has data.

### 1.4 Build + test silver, verify dedup (Task 4 Steps 4–6)

```bash
uv run dbt build --select silver_metrics
```
Expected: Glue table `silver.silver_metrics` (Iceberg) created on first run (full bronze scan), then tests pass (not_null + unique-combination).

Verify zero duplicate keys (run in Athena console or via `aws athena start-query-execution`):
```sql
select count(*) as dup_rows from (
  select measurement, cdevice, pdevice, parameter, ts_ns, count(*) c
  from silver.silver_metrics
  group by 1,2,3,4,5 having count(*) > 1
);
```
Expected: `dup_rows = 0`.

Idempotency check — re-run and confirm nothing duplicates:
```bash
uv run dbt run --select silver_metrics
```
Re-run the dedup query → still `dup_rows = 0`, row count stable.

### 1.5 Build + test gold, spot-check aggregates (Task 5 Steps 4–5)

```bash
uv run dbt build --select gold_metrics_daily
```
Expected: Glue table `gold.gold_metrics_daily` (Iceberg) created; unique-combination + `sample_count >= 1` tests pass.

Spot-check an aggregate against silver:
```sql
select g.avg_value, s.avg_value as recomputed
from gold.gold_metrics_daily g
join (
  select measurement, cdevice, pdevice, parameter, dt, avg(value) avg_value
  from silver.silver_metrics group by 1,2,3,4,5
) s using (measurement, cdevice, pdevice, parameter, dt)
limit 20;
```
Expected: `avg_value` == `recomputed` for sampled rows.

### 1.6 Full build — exactly what the container runs (Task 6)

```bash
uv run dbt build
```
Expected: silver → gold in dependency order, all tests pass, source freshness OK.

Confirm both tables are Iceberg + partitioned by `dt`:
```sql
show create table silver.silver_metrics;
show create table gold.gold_metrics_daily;
```
Expected: both show `table_type='ICEBERG'` and `partitioned by (dt)`.

---

## Part 2 — Deploy to ECS + schedule (plan Task 10)

### 2.1 Fill the env files (from the committed templates)

```bash
cp deploy/config.env.example deploy/config.env   # account/region/roles/network/bucket/schedule
cp deploy/task.env.example   deploy/task.env     # container runtime env (Athena dirs/workgroup)
```
Edit both, replacing every `<...>` placeholder. Both real files are gitignored.

- `deploy/config.env` is **sourced by the deploy scripts** (`set -a; source …`).
- `deploy/task.env` is **uploaded to S3** and read by the task via ECS `environmentFiles`. It must be
  plain `KEY=VALUE` lines — **no quotes, no `export`** (quotes become part of the value). Changing it
  and re-uploading takes effect on the **next** task run, not running tasks.

### 2.2 Create the IAM roles (one-time)

```bash
set -a; source deploy/config.env; set +a   # provides DATA_BUCKET, *_ROLE_ARN, ENV_FILE_S3_ARN, etc.

# execution role (image pull + logs + read the S3 env file)
aws iam create-role --role-name s3-pipeline-dbt-exec \
  --assume-role-policy-document file://deploy/iam/ecs-tasks-trust.json
aws iam attach-role-policy --role-name s3-pipeline-dbt-exec \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
aws iam put-role-policy --role-name s3-pipeline-dbt-exec \
  --policy-name envfile --policy-document "$(envsubst < deploy/iam/exec-envfile-permissions.json)"

# task role (Athena + Glue + S3 data)
aws iam create-role --role-name s3-pipeline-dbt-task \
  --assume-role-policy-document file://deploy/iam/ecs-tasks-trust.json
aws iam put-role-policy --role-name s3-pipeline-dbt-task \
  --policy-name perms --policy-document "$(envsubst < deploy/iam/task-permissions.json)"

# scheduler role (RunTask + PassRole)
aws iam create-role --role-name s3-pipeline-dbt-scheduler \
  --assume-role-policy-document file://deploy/iam/scheduler-trust.json
aws iam put-role-policy --role-name s3-pipeline-dbt-scheduler \
  --policy-name perms --policy-document "$(envsubst < deploy/iam/scheduler-permissions.json)"
```
The role names above match the defaults in `config.env.example` (`*_ROLE_ARN`). `envsubst` substitutes
`${DATA_BUCKET}` / `${ENV_FILE_S3_ARN}` / `${EXEC_ROLE_ARN}` / `${TASK_ROLE_ARN}` from `config.env`.

### 2.3 Build + push the image, register the task

```bash
deploy/scripts/build_and_push.sh   # ensures ECR repo, docker build (root context, -f deploy/Dockerfile), push
deploy/scripts/register_task.sh    # uploads deploy/task.env to S3, renders task-definition.json, registers
```

### 2.4 Run once and watch logs

```bash
deploy/scripts/run_once.sh
aws logs tail "$LOG_GROUP" --follow --region "$AWS_REGION"
```
Expected: task goes RUNNING → STOPPED exit 0; logs show `dbt build --target prod` completing with
silver + gold built and tests passing.

> If the task fails immediately with `ResourceInitializationError` reading the env file, the **execution
> role** is missing `s3:GetObject` on `ENV_FILE_S3_ARN` (re-check §2.2 exec role).

### 2.5 Create the daily schedule

```bash
deploy/scripts/create_schedule.sh   # EventBridge Scheduler `s3-pipeline-dbt-daily` using SCHEDULE_EXPRESSION
```
Default cron is `cron(0 1 * * ? *)` (01:00 UTC daily) — adjust `SCHEDULE_EXPRESSION` in `config.env`.

### 2.6 Confirm idempotent scheduled run

Wait for the next fire (or trigger once more), re-check CloudWatch logs and re-run the §1.4 dedup query.
Expected: exit 0, `dup_rows = 0`, gold counts consistent — the periodic MERGE is idempotent.

---

## Appendix A — bounding the first-run bronze scan (optional)

If bronze is large and you want the initial silver build to start from a date instead of scanning all
history, add a `start_date` floor to the first `dt` filter in `models/silver/silver_metrics.sql`'s `src`
CTE (and pass `--vars '{start_date: "2026-06-01"}'`), then remove it once seeded. The incremental
`lookback_days` filter already bounds every run after the first. This is intentionally left out of the
default models to keep the PoC simple.

## Appendix B — env var reference

| var | used by | meaning |
|-----|---------|---------|
| `AWS_REGION` | dbt profile, scripts, task | AWS region |
| `ATHENA_STAGING_DIR` | dbt profile / task.env | Athena query-results S3 dir |
| `ATHENA_DATA_DIR` | dbt profile / task.env | S3 data dir for built tables |
| `ATHENA_WORKGROUP` | dbt profile / task.env | Athena workgroup |
| `DBT_SCHEMA` | dbt profile | default output Glue DB (`silver`; gold overrides) |
| `DBT_TARGET` | dbt profile | `dev` locally, `prod` in the container |
| `DATA_BUCKET` | IAM envsubst | bucket name for task/exec S3 policies |
| `lookback_days` (dbt var) | models | incremental window in days (default 3) |
