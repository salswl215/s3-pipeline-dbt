# silver/gold dbt-athena PoC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the silver/gold medallion layer with dbt + Athena on top of the existing `bronze.metrics` Iceberg table, runnable locally against real Athena and deployable as a periodic ECS Fargate task.

**Architecture:** A uv-managed dbt project (`dbt-athena` adapter) declares `bronze.metrics` as a source, materializes `silver_metrics` (dedup + typed) and `gold_metrics_daily` (daily aggregate) as Iceberg incremental models using the `merge` strategy, each bounded by a `lookback_days` partition filter for idempotent, cost-capped runs. A Docker image runs `dbt build`; an ECS Fargate task definition + EventBridge Scheduler trigger it on a daily cron, mirroring the sibling `../s3-pipeline/deploy` pattern.

**Tech Stack:** Python 3.12, uv, dbt-core, dbt-athena, dbt-utils, AWS Athena (Iceberg/Glue), ECS Fargate, EventBridge Scheduler, Docker.

---

## Prerequisites (operator must have before running)

- AWS credentials with Athena + Glue + S3 access in the target account/region.
- `bronze.metrics` Iceberg table exists and has data (confirmed live).
- An Athena workgroup and two S3 prefixes: a staging dir and a data dir for silver/gold.
- Tools installed: `uv`, `docker`, `aws` CLI, `envsubst` (gettext).

## File Structure

| File | Responsibility |
|------|----------------|
| `pyproject.toml` | uv project + dbt deps |
| `dbt_project.yml` | dbt project config: model paths, per-folder `+schema`, `vars.lookback_days` |
| `packages.yml` | dbt-utils dependency |
| `profiles.yml` | Athena connection via `env_var()` (committed, values from env) |
| `macros/generate_schema_name.sql` | Use custom schema name verbatim (no target prefix) |
| `models/bronze/_bronze__source.yml` | Declare `source('bronze','metrics')` + freshness |
| `models/silver/silver_metrics.sql` | dedup + typed, Iceberg incremental merge |
| `models/silver/_silver__models.yml` | silver tests (not_null, unique combination) |
| `models/gold/gold_metrics_daily.sql` | daily aggregate, Iceberg incremental merge |
| `models/gold/_gold__models.yml` | gold tests (unique combination, range) |
| `deploy/Dockerfile` + `.dockerignore` | container running `dbt build` |
| `deploy/task-definition.json` | Fargate task def, S3 env file |
| `deploy/config.env.example` / `task.env.example` | deploy-tool vs runtime env |
| `deploy/iam/*.json` | trust + permission policies |
| `deploy/scripts/*.sh` | build/push, register, run-once, schedule |

---

## Task 1: uv + dbt project scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `packages.yml`
- Create: `dbt_project.yml`

- [ ] **Step 1: Initialize uv project and add dbt deps**

Run:
```bash
uv init --no-readme --name s3-pipeline-dbt
uv add dbt-core dbt-athena
```
Expected: `pyproject.toml` + `uv.lock` created; `dbt --version` available via `uv run dbt --version`.

- [ ] **Step 2: Remove the stub module uv created**

Run:
```bash
rm -f main.py hello.py
```
Expected: no stray top-level Python stub (this is a dbt project, not a Python package).

- [ ] **Step 3: Create `packages.yml`**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
```

- [ ] **Step 4: Create `dbt_project.yml`**

```yaml
name: 's3_pipeline_dbt'
version: '0.1.0'
config-version: 2
profile: 's3_pipeline_dbt'

model-paths: ["models"]
macro-paths: ["macros"]

vars:
  lookback_days: 3

models:
  s3_pipeline_dbt:
    silver:
      +schema: silver
    gold:
      +schema: gold
```

- [ ] **Step 5: Verify dbt parses the project (will fail on profile — expected)**

Run: `uv run dbt parse`
Expected: FAIL — `Could not find profile named 's3_pipeline_dbt'` (profiles.yml comes in Task 2). This confirms the project file is otherwise valid.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock packages.yml dbt_project.yml
git commit -m "feat: scaffold uv + dbt-athena project"
```

---

## Task 2: Athena profile + custom schema macro

**Files:**
- Create: `profiles.yml`
- Create: `macros/generate_schema_name.sql`

- [ ] **Step 1: Create `profiles.yml` (values from env, safe to commit)**

```yaml
s3_pipeline_dbt:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  outputs:
    dev:
      type: athena
      region_name: "{{ env_var('AWS_REGION') }}"
      database: awsdatacatalog
      schema: "{{ env_var('DBT_SCHEMA', 'silver') }}"
      s3_staging_dir: "{{ env_var('ATHENA_STAGING_DIR') }}"
      s3_data_dir: "{{ env_var('ATHENA_DATA_DIR') }}"
      work_group: "{{ env_var('ATHENA_WORKGROUP') }}"
      threads: 4
    prod:
      type: athena
      region_name: "{{ env_var('AWS_REGION') }}"
      database: awsdatacatalog
      schema: "{{ env_var('DBT_SCHEMA', 'silver') }}"
      s3_staging_dir: "{{ env_var('ATHENA_STAGING_DIR') }}"
      s3_data_dir: "{{ env_var('ATHENA_DATA_DIR') }}"
      work_group: "{{ env_var('ATHENA_WORKGROUP') }}"
      threads: 4
```

- [ ] **Step 2: Create `macros/generate_schema_name.sql`**

This makes `+schema: silver|gold` produce Glue databases named exactly `silver`/`gold` (not `<target_schema>_silver`).

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

- [ ] **Step 3: Export env and verify the connection against real Athena**

Run:
```bash
export AWS_REGION=<region>
export ATHENA_STAGING_DIR=s3://<bucket>/_athena/staging/
export ATHENA_DATA_DIR=s3://<bucket>/silver/
export ATHENA_WORKGROUP=<workgroup>
export DBT_SCHEMA=silver
uv run dbt deps
uv run dbt debug
```
Expected: `dbt deps` installs dbt_utils; `dbt debug` reports `Connection test: OK` (Athena reachable, catalog + staging accessible).

- [ ] **Step 4: Commit**

```bash
git add profiles.yml macros/generate_schema_name.sql
git commit -m "feat: add Athena profile and custom schema macro"
```

---

## Task 3: bronze source declaration

**Files:**
- Create: `models/bronze/_bronze__source.yml`

- [ ] **Step 1: Declare the bronze source**

```yaml
version: 2

sources:
  - name: bronze
    description: "Raw->bronze Iceberg metrics produced by ../s3-pipeline. Read-only contract."
    schema: bronze
    tables:
      - name: metrics
        description: "Append-only event metrics, partitioned by event-date dt."
        loaded_at_field: ts
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 72, period: hour}
        columns:
          - name: cdevice
            description: "tag (device)"
          - name: pdevice
            description: "tag (device)"
          - name: parameter
            description: "tag (metric name)"
          - name: value
            description: "field value (double)"
          - name: ts
            description: "event time, microsecond, UTC (timestamptz)"
          - name: ts_ns
            description: "original ns epoch (lossless, bigint)"
          - name: measurement
            description: "line-protocol measurement"
          - name: dt
            description: "event-date partition key (UTC date of ts)"
```

- [ ] **Step 2: Verify the source resolves and is queryable in Athena**

Run:
```bash
uv run dbt parse
uv run dbt source freshness
```
Expected: `dbt parse` succeeds; `source freshness` runs a real Athena query against `bronze.metrics` and reports a freshness state (PASS/WARN) — confirms the source is correctly wired and has data.

- [ ] **Step 3: Commit**

```bash
git add models/bronze/_bronze__source.yml
git commit -m "feat: declare bronze.metrics source"
```

---

## Task 4: silver_metrics model + tests

**Files:**
- Create: `models/silver/silver_metrics.sql`
- Create: `models/silver/_silver__models.yml`

- [ ] **Step 1: Write the silver tests first (they will fail — model does not exist yet)**

`models/silver/_silver__models.yml`:
```yaml
version: 2

models:
  - name: silver_metrics
    description: "Deduplicated, typed metrics. One row per natural key (measurement+devices+parameter+ts_ns)."
    columns:
      - name: value
        tests: [not_null]
      - name: ts
        tests: [not_null]
      - name: ts_ns
        tests: [not_null]
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - measurement
            - cdevice
            - pdevice
            - parameter
            - ts_ns
```

- [ ] **Step 2: Run the test to confirm it fails (model missing)**

Run: `uv run dbt test --select silver_metrics`
Expected: FAIL/ERROR — `silver_metrics` is not a node / relation does not exist. Confirms tests target the right model.

- [ ] **Step 3: Write `models/silver/silver_metrics.sql`**

```sql
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['measurement','cdevice','pdevice','parameter','ts_ns'],
    partitioned_by=['dt']
) }}

with src as (
  select cdevice, pdevice, parameter, value, ts, ts_ns, measurement, dt
  from {{ source('bronze','metrics') }}
  {% if is_incremental() %}
  where dt >= (select date_add('day', -{{ var('lookback_days', 3) }}, max(dt)) from {{ this }})
  {% endif %}
),
ranked as (
  select *,
    row_number() over (
      partition by measurement, cdevice, pdevice, parameter, ts_ns
      order by ts
    ) as rn
  from src
)
select cdevice, pdevice, parameter, value, ts, ts_ns, measurement, dt
from ranked
where rn = 1
```

- [ ] **Step 4: Build the model and run its tests against real Athena**

Run: `uv run dbt build --select silver_metrics`
Expected: PASS — creates Glue table `silver.silver_metrics` (Iceberg) on first run (full bronze scan), then runs tests; unique combination + not_null tests pass.

- [ ] **Step 5: Verify dedup correctness with a direct Athena query**

Run (Athena console or `aws athena start-query-execution`):
```sql
select count(*) as dup_rows from (
  select measurement, cdevice, pdevice, parameter, ts_ns, count(*) c
  from silver.silver_metrics
  group by 1,2,3,4,5 having count(*) > 1
);
```
Expected: `dup_rows = 0`.

- [ ] **Step 6: Verify incremental merge is idempotent (second run changes nothing)**

Run: `uv run dbt run --select silver_metrics`
Then re-run the dedup query from Step 5.
Expected: still `dup_rows = 0`; row count stable (re-merging the lookback window does not duplicate).

- [ ] **Step 7: Commit**

```bash
git add models/silver/silver_metrics.sql models/silver/_silver__models.yml
git commit -m "feat: add silver_metrics dedup model"
```

---

## Task 5: gold_metrics_daily model + tests

**Files:**
- Create: `models/gold/gold_metrics_daily.sql`
- Create: `models/gold/_gold__models.yml`

- [ ] **Step 1: Write the gold tests first (will fail — model does not exist yet)**

`models/gold/_gold__models.yml`:
```yaml
version: 2

models:
  - name: gold_metrics_daily
    description: "Daily aggregate per device+parameter+measurement. avg/min/max/count of value."
    columns:
      - name: sample_count
        tests:
          - dbt_utils.accepted_range:
              min_value: 1
              inclusive: true
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - measurement
            - cdevice
            - pdevice
            - parameter
            - dt
```

- [ ] **Step 2: Run the test to confirm it fails (model missing)**

Run: `uv run dbt test --select gold_metrics_daily`
Expected: FAIL/ERROR — `gold_metrics_daily` not found. Confirms tests target the right model.

- [ ] **Step 3: Write `models/gold/gold_metrics_daily.sql`**

```sql
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='merge',
    unique_key=['measurement','cdevice','pdevice','parameter','dt'],
    partitioned_by=['dt']
) }}

select
  measurement, cdevice, pdevice, parameter, dt,
  avg(value)  as avg_value,
  min(value)  as min_value,
  max(value)  as max_value,
  count(*)    as sample_count
from {{ ref('silver_metrics') }}
{% if is_incremental() %}
where dt >= (select date_add('day', -{{ var('lookback_days', 3) }}, max(dt)) from {{ this }})
{% endif %}
group by 1,2,3,4,5
```

- [ ] **Step 4: Build the model and run its tests against real Athena**

Run: `uv run dbt build --select gold_metrics_daily`
Expected: PASS — creates Glue table `gold.gold_metrics_daily` (Iceberg); unique combination + range tests pass.

- [ ] **Step 5: Spot-check an aggregate against silver with a direct Athena query**

Run:
```sql
select g.avg_value, s.avg_value as recomputed
from gold.gold_metrics_daily g
join (
  select measurement, cdevice, pdevice, parameter, dt, avg(value) avg_value
  from silver.silver_metrics group by 1,2,3,4,5
) s using (measurement, cdevice, pdevice, parameter, dt)
limit 20;
```
Expected: `avg_value` equals `recomputed` for sampled rows.

- [ ] **Step 6: Commit**

```bash
git add models/gold/gold_metrics_daily.sql models/gold/_gold__models.yml
git commit -m "feat: add gold_metrics_daily aggregate model"
```

---

## Task 6: Full-build verification (local E2E)

**Files:** none (verification task)

- [ ] **Step 1: Run the complete build the ECS entrypoint will run**

Run: `uv run dbt build`
Expected: PASS — silver → gold run in dependency order, all tests pass, source freshness OK. This is exactly what the container executes.

- [ ] **Step 2: Confirm both Glue tables are Iceberg and partitioned by dt**

Run:
```sql
show create table silver.silver_metrics;
show create table gold.gold_metrics_daily;
```
Expected: both show `table_type='ICEBERG'` and `partitioned by (dt)`.

- [ ] **Step 3: Tag the working local PoC**

```bash
git tag poc-local-verified
```

---

## Task 7: Container image (`dbt build`)

**Files:**
- Create: `deploy/Dockerfile`
- Create: `deploy/Dockerfile.dockerignore`

- [ ] **Step 1: Create `deploy/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

FROM python:3.12-slim-bookworm AS runtime
RUN useradd --create-home --uid 10001 appuser
WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
# dbt project files
COPY --chown=appuser:appuser dbt_project.yml packages.yml profiles.yml ./
COPY --chown=appuser:appuser models ./models
COPY --chown=appuser:appuser macros ./macros
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app
# install dbt packages (dbt_utils) into the image
RUN dbt deps
USER appuser
ENTRYPOINT ["dbt", "build", "--target", "prod"]
CMD []
```

- [ ] **Step 2: Create `deploy/Dockerfile.dockerignore`**

```
.venv
target
dbt_packages
logs
.git
.bkit
docs
deploy
**/__pycache__
```

- [ ] **Step 3: Build the image locally**

Run: `docker build -f deploy/Dockerfile -t s3-pipeline-dbt:local .`
Expected: image builds; `dbt deps` runs during build.

- [ ] **Step 4: Smoke-test the image against Athena with local creds**

Run:
```bash
docker run --rm \
  -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
  -e ATHENA_STAGING_DIR -e ATHENA_DATA_DIR -e ATHENA_WORKGROUP \
  -e DBT_SCHEMA=silver -e DBT_TARGET=prod \
  s3-pipeline-dbt:local
```
Expected: container runs `dbt build --target prod` successfully against real Athena.

- [ ] **Step 5: Commit**

```bash
git add deploy/Dockerfile deploy/Dockerfile.dockerignore
git commit -m "feat: add dbt build container image"
```

---

## Task 8: Deploy env templates + IAM policies

**Files:**
- Create: `deploy/config.env.example`
- Create: `deploy/task.env.example`
- Create: `deploy/iam/ecs-tasks-trust.json`
- Create: `deploy/iam/task-permissions.json`
- Create: `deploy/iam/exec-envfile-permissions.json`
- Create: `deploy/iam/scheduler-trust.json`
- Create: `deploy/iam/scheduler-permissions.json`

- [ ] **Step 1: Create `deploy/config.env.example` (deploy-tool vars)**

```bash
# AWS / account
AWS_REGION=<region>
AWS_ACCOUNT_ID=<account-id>
# ECR / image
ECR_REPO=s3-pipeline-dbt
ECR_IMAGE_URI=<account-id>.dkr.ecr.<region>.amazonaws.com/s3-pipeline-dbt:latest
# ECS
ECS_CLUSTER=<cluster-name>
TASK_FAMILY=s3-pipeline-dbt
EXEC_ROLE_ARN=arn:aws:iam::<account-id>:role/s3-pipeline-dbt-exec
TASK_ROLE_ARN=arn:aws:iam::<account-id>:role/s3-pipeline-dbt-task
SCHEDULER_ROLE_ARN=arn:aws:iam::<account-id>:role/s3-pipeline-dbt-scheduler
# networking (awsvpc)
SUBNETS=subnet-aaa,subnet-bbb
SECURITY_GROUPS=sg-aaa
# logs
LOG_GROUP=/ecs/s3-pipeline-dbt
# runtime env file location in S3 (uploaded from task.env)
ENV_FILE_S3_URI=s3://<bucket>/_config/s3-pipeline-dbt/task.env
ENV_FILE_S3_ARN=arn:aws:s3:::<bucket>/_config/s3-pipeline-dbt/task.env
# schedule
SCHEDULE_EXPRESSION=cron(0 1 * * ? *)
```

- [ ] **Step 2: Create `deploy/task.env.example` (container runtime env — ECS environmentFiles format, KEY=VALUE, no quotes/export)**

```
AWS_REGION=<region>
DBT_TARGET=prod
DBT_SCHEMA=silver
ATHENA_STAGING_DIR=s3://<bucket>/_athena/staging/
ATHENA_DATA_DIR=s3://<bucket>/silver/
ATHENA_WORKGROUP=<workgroup>
```

- [ ] **Step 3: Create `deploy/iam/ecs-tasks-trust.json`**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Principal": { "Service": "ecs-tasks.amazonaws.com" },
      "Action": "sts:AssumeRole" }
  ]
}
```

- [ ] **Step 4: Create `deploy/iam/task-permissions.json` (Athena + Glue + S3)**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "Athena", "Effect": "Allow",
      "Action": ["athena:StartQueryExecution","athena:GetQueryExecution","athena:GetQueryResults","athena:StopQueryExecution","athena:GetWorkGroup"],
      "Resource": "*" },
    { "Sid": "Glue", "Effect": "Allow",
      "Action": ["glue:GetDatabase","glue:GetDatabases","glue:CreateDatabase","glue:GetTable","glue:GetTables","glue:CreateTable","glue:UpdateTable","glue:DeleteTable","glue:BatchCreatePartition","glue:GetPartition","glue:GetPartitions","glue:BatchGetPartition","glue:UpdatePartition","glue:CreatePartition"],
      "Resource": "*" },
    { "Sid": "S3DataRW", "Effect": "Allow",
      "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::${DATA_BUCKET}/silver/*","arn:aws:s3:::${DATA_BUCKET}/gold/*","arn:aws:s3:::${DATA_BUCKET}/bronze/*","arn:aws:s3:::${DATA_BUCKET}/_athena/*"] },
    { "Sid": "S3List", "Effect": "Allow",
      "Action": ["s3:GetBucketLocation","s3:ListBucket"],
      "Resource": "arn:aws:s3:::${DATA_BUCKET}" }
  ]
}
```

> Note: `${DATA_BUCKET}` is substituted by `envsubst` at apply time (add `DATA_BUCKET=<bucket>` to `config.env`). bronze prefix is read-only in practice but is granted RW here for Athena Iceberg metadata simplicity in the PoC; tighten later.

- [ ] **Step 5: Create `deploy/iam/exec-envfile-permissions.json` (execution role reads the S3 env file)**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["s3:GetObject"], "Resource": "${ENV_FILE_S3_ARN}" },
    { "Effect": "Allow", "Action": ["s3:GetBucketLocation"], "Resource": "arn:aws:s3:::${DATA_BUCKET}" }
  ]
}
```

- [ ] **Step 6: Create `deploy/iam/scheduler-trust.json`**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Principal": { "Service": "scheduler.amazonaws.com" },
      "Action": "sts:AssumeRole" }
  ]
}
```

- [ ] **Step 7: Create `deploy/iam/scheduler-permissions.json` (scheduler runs the task)**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["ecs:RunTask"], "Resource": "*" },
    { "Effect": "Allow", "Action": ["iam:PassRole"], "Resource": ["${EXEC_ROLE_ARN}","${TASK_ROLE_ARN}"] }
  ]
}
```

- [ ] **Step 8: Commit**

```bash
git add deploy/config.env.example deploy/task.env.example deploy/iam
git commit -m "feat: add deploy env templates and IAM policies"
```

---

## Task 9: ECS task definition + deploy scripts

**Files:**
- Create: `deploy/task-definition.json`
- Create: `deploy/scripts/_common.sh`
- Create: `deploy/scripts/build_and_push.sh`
- Create: `deploy/scripts/register_task.sh`
- Create: `deploy/scripts/run_once.sh`
- Create: `deploy/scripts/create_schedule.sh`

- [ ] **Step 1: Create `deploy/task-definition.json`**

```json
{
  "family": "${TASK_FAMILY}",
  "requiresCompatibilities": ["FARGATE"],
  "networkMode": "awsvpc",
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "${EXEC_ROLE_ARN}",
  "taskRoleArn": "${TASK_ROLE_ARN}",
  "runtimePlatform": { "cpuArchitecture": "X86_64", "operatingSystemFamily": "LINUX" },
  "containerDefinitions": [
    {
      "name": "s3-pipeline-dbt",
      "image": "${ECR_IMAGE_URI}",
      "essential": true,
      "environmentFiles": [ { "type": "s3", "value": "${ENV_FILE_S3_ARN}" } ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "${LOG_GROUP}",
          "awslogs-region": "${AWS_REGION}",
          "awslogs-stream-prefix": "s3-pipeline-dbt",
          "awslogs-create-group": "true"
        }
      }
    }
  ]
}
```

- [ ] **Step 2: Create `deploy/scripts/_common.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/config.env"; set +a
```

- [ ] **Step 3: Create `deploy/scripts/build_and_push.sh`**

```bash
#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
aws ecr describe-repositories --repository-names "$ECR_REPO" --region "$AWS_REGION" >/dev/null 2>&1 \
  || aws ecr create-repository --repository-name "$ECR_REPO" --region "$AWS_REGION"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
docker build -f "$DEPLOY_DIR/Dockerfile" -t "$ECR_IMAGE_URI" "$ROOT_DIR"
docker push "$ECR_IMAGE_URI"
```

- [ ] **Step 4: Create `deploy/scripts/register_task.sh`**

```bash
#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
# upload runtime env file to S3
aws s3 cp "$DEPLOY_DIR/task.env" "$ENV_FILE_S3_URI"
# render + register task definition
envsubst < "$DEPLOY_DIR/task-definition.json" > /tmp/s3-pipeline-dbt-taskdef.json
aws ecs register-task-definition --region "$AWS_REGION" \
  --cli-input-json file:///tmp/s3-pipeline-dbt-taskdef.json
```

- [ ] **Step 5: Create `deploy/scripts/run_once.sh`**

```bash
#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
aws ecs run-task --region "$AWS_REGION" \
  --cluster "$ECS_CLUSTER" \
  --launch-type FARGATE \
  --task-definition "$TASK_FAMILY" \
  --network-configuration "awsvpcConfiguration={subnets=[${SUBNETS}],securityGroups=[${SECURITY_GROUPS}],assignPublicIp=ENABLED}"
```

- [ ] **Step 6: Create `deploy/scripts/create_schedule.sh`**

```bash
#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
TASK_DEF_ARN="$(aws ecs describe-task-definition --region "$AWS_REGION" \
  --task-definition "$TASK_FAMILY" --query 'taskDefinition.taskDefinitionArn' --output text)"
aws scheduler create-schedule --region "$AWS_REGION" \
  --name s3-pipeline-dbt-daily \
  --schedule-expression "$SCHEDULE_EXPRESSION" \
  --flexible-time-window '{"Mode":"OFF"}' \
  --target "{
    \"Arn\": \"arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:cluster/${ECS_CLUSTER}\",
    \"RoleArn\": \"${SCHEDULER_ROLE_ARN}\",
    \"EcsParameters\": {
      \"TaskDefinitionArn\": \"${TASK_DEF_ARN}\",
      \"LaunchType\": \"FARGATE\",
      \"NetworkConfiguration\": {\"awsvpcConfiguration\": {\"Subnets\": [${SUBNETS//,/\",\"}], \"SecurityGroups\": [${SECURITY_GROUPS//,/\",\"}], \"AssignPublicIp\": \"ENABLED\"}}
    }
  }"
```

> Note: `SUBNETS`/`SECURITY_GROUPS` in `config.env` are comma-separated bare ids; the `run_task` form passes them as-is, the scheduler form quotes them. If you have multiple, ensure they are valid JSON arrays after substitution.

- [ ] **Step 7: Make scripts executable and commit**

```bash
chmod +x deploy/scripts/*.sh
git add deploy/task-definition.json deploy/scripts
git commit -m "feat: add ECS task definition and deploy scripts"
```

---

## Task 10: Deploy + scheduled-run verification (real AWS)

**Files:** none (verification task). Operator fills `deploy/config.env` and `deploy/task.env` from the `.example` files first.

- [ ] **Step 1: Create IAM roles (one-time)**

Run:
```bash
set -a; source deploy/config.env; set +a
export DATA_BUCKET=<bucket>
# exec role
aws iam create-role --role-name s3-pipeline-dbt-exec --assume-role-policy-document file://deploy/iam/ecs-tasks-trust.json
aws iam attach-role-policy --role-name s3-pipeline-dbt-exec --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
aws iam put-role-policy --role-name s3-pipeline-dbt-exec --policy-name envfile --policy-document "$(envsubst < deploy/iam/exec-envfile-permissions.json)"
# task role
aws iam create-role --role-name s3-pipeline-dbt-task --assume-role-policy-document file://deploy/iam/ecs-tasks-trust.json
aws iam put-role-policy --role-name s3-pipeline-dbt-task --policy-name perms --policy-document "$(envsubst < deploy/iam/task-permissions.json)"
# scheduler role
aws iam create-role --role-name s3-pipeline-dbt-scheduler --assume-role-policy-document file://deploy/iam/scheduler-trust.json
aws iam put-role-policy --role-name s3-pipeline-dbt-scheduler --policy-name perms --policy-document "$(envsubst < deploy/iam/scheduler-permissions.json)"
```
Expected: three roles created with inline policies attached.

- [ ] **Step 2: Build + push the image**

Run: `deploy/scripts/build_and_push.sh`
Expected: ECR repo exists, image pushed to `$ECR_IMAGE_URI`.

- [ ] **Step 3: Register the task definition (uploads task.env to S3)**

Run: `deploy/scripts/register_task.sh`
Expected: `task.env` uploaded to `$ENV_FILE_S3_URI`; task definition `s3-pipeline-dbt` registered.

- [ ] **Step 4: Run once and watch logs**

Run: `deploy/scripts/run_once.sh`
Then tail CloudWatch:
```bash
aws logs tail "$LOG_GROUP" --follow --region "$AWS_REGION"
```
Expected: task reaches RUNNING then STOPPED with exit 0; logs show `dbt build --target prod` completing with silver+gold built and tests passing.

- [ ] **Step 5: Create the daily schedule**

Run: `deploy/scripts/create_schedule.sh`
Expected: EventBridge schedule `s3-pipeline-dbt-daily` created with the cron expression.

- [ ] **Step 6: Confirm idempotent scheduled run**

Wait for (or manually invoke) the next scheduled run; re-check CloudWatch logs and re-run the dedup query from Task 4 Step 5.
Expected: exit 0, `dup_rows = 0`, gold counts consistent — the periodic merge is idempotent.

- [ ] **Step 7: Write the deploy README and commit**

Create `deploy/README.md` documenting the order: fill `config.env`/`task.env` → create roles → `build_and_push.sh` → `register_task.sh` → `run_once.sh` → `create_schedule.sh`.

```bash
git add deploy/README.md
git commit -m "docs: add deploy runbook"
```

---

## Self-Review

**1. Spec coverage:**
- §2 어댑터/Iceberg/merge/lookback → Tasks 1,2,4,5. ✅
- §3 bronze source 계약 → Task 3. ✅
- §4.2 silver dedup → Task 4. ✅
- §4.3 gold daily → Task 5. ✅
- §4.4 source/tests → Tasks 3,4,5. ✅
- §4.5 레이아웃 / §4.6 profile + generate_schema_name → Tasks 1,2. ✅
- §5 Dockerfile/task-def/IAM/Scheduler → Tasks 7,8,9,10. ✅
- §6 검증(E2E 1~5) → Tasks 4 (build+dedup), 6 (full build), 10 (run_once + schedule). ✅
- §7 first-run scan / lookback / merge cost → noted in Task 4 Step 4 and plan prose. ✅

**2. Placeholder scan:** Account-specific values appear only inside `*.example` env files and IAM `${VAR}` substitution targets (intended config, not plan gaps). No "TBD/TODO/implement later". ✅

**3. Type/name consistency:** silver unique_key `[measurement,cdevice,pdevice,parameter,ts_ns]` identical across config (Task 4 Step 3), test (Task 4 Step 1), and dedup query (Task 4 Step 5). gold key `[measurement,cdevice,pdevice,parameter,dt]` identical across config, test, spot-check. Env var names (`ATHENA_STAGING_DIR`, `ATHENA_DATA_DIR`, `ATHENA_WORKGROUP`, `DBT_SCHEMA`, `DBT_TARGET`) consistent across profiles.yml, Dockerfile, task.env.example. Profile name `s3_pipeline_dbt` consistent in dbt_project.yml + profiles.yml. ✅
