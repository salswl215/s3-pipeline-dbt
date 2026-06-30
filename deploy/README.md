# 배포 & AWS 검증 런북

이 런북은 **실제 AWS 자격증명 / 라이브 Athena**가 필요한 모든 단계를 다룹니다 — dbt 코드를
작성하고 정적으로 검증하는 동안 의도적으로 미뤄둔 부분들입니다. AWS 접근 권한이 준비되면
아래 절차를 실행하세요.

프로젝트 진입점: 컨테이너는 `dbt build --target prod`(silver → gold + 전체 테스트)를 실행합니다.

---

## 0. 사전 준비

- 대상 계정/리전에 대한 셸의 AWS 자격증명 (예: `aws sts get-caller-identity`가 동작해야 함).
- `bronze.metrics` Iceberg 테이블이 존재하고 데이터가 있을 것 (이미 라이브 상태).
- 쿼리 결과가 활성화된 Athena **워크그룹**, 그리고 두 개의 S3 위치:
  - **staging dir** (Athena 쿼리 결과), 예: `s3://<bucket>/_athena/staging/`
  - silver/gold 테이블 데이터용 **data dir**, 예: `s3://<bucket>/silver/` (gold는 `+schema`를 통해 `…/gold/` 아래에 기록)
- 도구: `uv`, `docker`, `aws` CLI, `envsubst`(gettext 패키지), `python3`.
- 기존 ECS 클러스터 + VPC 서브넷 + 보안 그룹 (태스크는 Fargate `awsvpc`로 실행됨).

> **비용 주의 (PoC):** 모델을 실행할 때마다 Athena를 스캔합니다. silver/gold는 `lookback_days`(기본 3)로
> 제한된 incremental MERGE라서 정상 운영 상태의 스캔은 작습니다. **첫 `dbt build`는 bronze 전체를 한 번
> 스캔합니다**(silver 시딩). bronze가 크다면 하한선을 설정하세요 — §A 참고.

---

## Part 1 — 실제 Athena 대상 로컬 검증

플랜 Task 2–6에서 미뤄둔 검증 단계입니다. repo 루트에서 실행하세요.

### 1.1 연결 env 내보내기

```bash
export AWS_REGION=<region>
export ATHENA_STAGING_DIR=s3://<bucket>/_athena/staging/
export ATHENA_DATA_DIR=s3://<bucket>/silver/
export ATHENA_WORKGROUP=<workgroup>
export DBT_SCHEMA=silver        # 기본 출력 스키마; gold 모델은 `gold`로 재정의
export DBT_TARGET=dev
```

### 1.2 패키지 설치 + 연결 테스트 (Task 2 Step 3)

```bash
uv run dbt deps
uv run dbt debug
```
기대 결과: `Connection test: OK` — Athena 접근 가능, catalog + staging 접근 가능.

### 1.3 bronze source가 resolve되는지 확인 (Task 3 Step 2)

```bash
uv run dbt source freshness
```
기대 결과: `bronze.metrics`에 대한 실제 Athena 쿼리가 freshness 상태(PASS/WARN)를 반환 — source가 연결되어 있고 데이터가 있음을 증명.

### 1.4 silver 빌드 + 테스트, dedup 검증 (Task 4 Steps 4–6)

```bash
uv run dbt build --select silver_metrics
```
기대 결과: 첫 실행에서 Glue 테이블 `silver.silver_metrics`(Iceberg) 생성(bronze 전체 스캔), 이후 테스트 통과(not_null + unique-combination).

중복 키가 0인지 검증 (Athena 콘솔 또는 `aws athena start-query-execution`로 실행):
```sql
select count(*) as dup_rows from (
  select measurement, cdevice, pdevice, parameter, ts_ns, count(*) c
  from silver.silver_metrics
  group by 1,2,3,4,5 having count(*) > 1
);
```
기대 결과: `dup_rows = 0`.

멱등성 확인 — 재실행해서 중복이 생기지 않는지 확인:
```bash
uv run dbt run --select silver_metrics
```
dedup 쿼리 재실행 → 여전히 `dup_rows = 0`, row 수 안정적.

### 1.5 gold 빌드 + 테스트, 집계 스팟 체크 (Task 5 Steps 4–5)

```bash
uv run dbt build --select gold_metrics_daily
```
기대 결과: Glue 테이블 `gold.gold_metrics_daily`(Iceberg) 생성; unique-combination + `sample_count >= 1` 테스트 통과.

silver와 비교해 집계값 스팟 체크:
```sql
select g.avg_value, s.avg_value as recomputed
from gold.gold_metrics_daily g
join (
  select measurement, cdevice, pdevice, parameter, dt, avg(value) avg_value
  from silver.silver_metrics group by 1,2,3,4,5
) s using (measurement, cdevice, pdevice, parameter, dt)
limit 20;
```
기대 결과: 샘플링한 row들에 대해 `avg_value` == `recomputed`.

### 1.6 전체 빌드 — 컨테이너가 실행하는 것과 정확히 동일 (Task 6)

```bash
uv run dbt build
```
기대 결과: 의존성 순서대로 silver → gold, 전체 테스트 통과, source freshness OK.

두 테이블 모두 Iceberg이고 `dt`로 파티셔닝됐는지 확인:
```sql
show create table silver.silver_metrics;
show create table gold.gold_metrics_daily;
```
기대 결과: 둘 다 `table_type='ICEBERG'`와 `partitioned by (dt)` 표시.

---

## Part 2 — ECS 배포 + 스케줄 (플랜 Task 10)

### 2.1 env 파일 채우기 (커밋된 템플릿으로부터)

```bash
cp deploy/config.env.example deploy/config.env   # 계정/리전/롤/네트워크/버킷/스케줄
cp deploy/task.env.example   deploy/task.env     # 컨테이너 런타임 env (Athena dirs/workgroup)
```
두 파일 모두 편집해서 모든 `<...>` 플레이스홀더를 교체하세요. 실제 파일 둘 다 gitignore 대상입니다.

- `deploy/config.env`는 **배포 스크립트가 source로 읽습니다** (`set -a; source …`).
- `deploy/task.env`는 **S3에 업로드되어** ECS `environmentFiles`를 통해 태스크가 읽습니다. 반드시
  순수 `KEY=VALUE` 라인이어야 하며 — **따옴표·`export` 금지**(따옴표가 값의 일부가 됨). 이 파일을
  변경하고 재업로드하면 실행 중인 태스크가 아니라 **다음** 태스크 실행부터 반영됩니다.

### 2.2 IAM 롤 생성 (1회)

```bash
set -a; source deploy/config.env; set +a   # DATA_BUCKET, *_ROLE_ARN, ENV_FILE_S3_ARN 등을 제공

# execution role (이미지 pull + 로그 + S3 env 파일 읽기)
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
위 롤 이름들은 `config.env.example`의 기본값(`*_ROLE_ARN`)과 일치합니다. `envsubst`는 `config.env`의
`${DATA_BUCKET}` / `${ENV_FILE_S3_ARN}` / `${EXEC_ROLE_ARN}` / `${TASK_ROLE_ARN}`를 치환합니다.

### 2.3 이미지 빌드 + 푸시, 태스크 등록

```bash
deploy/scripts/build_and_push.sh   # ECR repo 보장, docker build (루트 컨텍스트, -f deploy/Dockerfile), push
deploy/scripts/register_task.sh    # deploy/task.env를 S3에 업로드, task-definition.json 렌더링, 등록
```

### 2.4 1회 실행 + 로그 확인

```bash
deploy/scripts/run_once.sh
aws logs tail "$LOG_GROUP" --follow --region "$AWS_REGION"
```
기대 결과: 태스크가 RUNNING → STOPPED exit 0; 로그에 `dbt build --target prod`가 silver + gold 빌드 및
테스트 통과로 완료되는 모습이 표시됨.

> 태스크가 env 파일을 읽다가 `ResourceInitializationError`로 즉시 실패하면, **execution role**에
> `ENV_FILE_S3_ARN`에 대한 `s3:GetObject`가 누락된 것입니다 (§2.2 exec role 재확인).

### 2.5 일일 스케줄 생성

```bash
deploy/scripts/create_schedule.sh   # SCHEDULE_EXPRESSION을 사용하는 EventBridge Scheduler `s3-pipeline-dbt-daily`
```
기본 cron은 `cron(0 1 * * ? *)`(매일 01:00 UTC) — `config.env`의 `SCHEDULE_EXPRESSION`로 조정하세요.

### 2.6 멱등적 스케줄 실행 확인

다음 발화(또는 한 번 더 트리거)를 기다린 뒤 CloudWatch 로그를 재확인하고 §1.4 dedup 쿼리를 다시 실행하세요.
기대 결과: exit 0, `dup_rows = 0`, gold 카운트 일관됨 — 주기적 MERGE가 멱등적임.

---

## 부록 A — 첫 실행의 bronze 스캔 제한 (선택)

bronze가 크고 초기 silver 빌드를 전체 히스토리 스캔 대신 특정 날짜부터 시작하고 싶다면,
`models/silver/silver_metrics.sql`의 `src` CTE에 있는 첫 `dt` 필터에 `start_date` 하한을 추가하고
(`--vars '{start_date: "2026-06-01"}'`로 전달), 시딩이 끝나면 제거하세요. incremental `lookback_days`
필터가 첫 실행 이후의 모든 실행을 이미 제한합니다. 이 기능은 PoC를 단순하게 유지하려고 기본
모델에서 의도적으로 빠져 있습니다.

## 부록 B — env 변수 레퍼런스

| 변수 | 사용처 | 의미 |
|-----|---------|---------|
| `AWS_REGION` | dbt profile, 스크립트, 태스크 | AWS 리전 |
| `ATHENA_STAGING_DIR` | dbt profile / task.env | Athena 쿼리 결과 S3 dir |
| `ATHENA_DATA_DIR` | dbt profile / task.env | 빌드된 테이블의 S3 data dir |
| `ATHENA_WORKGROUP` | dbt profile / task.env | Athena 워크그룹 |
| `DBT_SCHEMA` | dbt profile | 기본 출력 Glue DB (`silver`; gold는 재정의) |
| `DBT_TARGET` | dbt profile | 로컬은 `dev`, 컨테이너는 `prod` |
| `DATA_BUCKET` | IAM envsubst | task/exec S3 정책용 버킷 이름 |
| `lookback_days` (dbt var) | 모델 | incremental 윈도우(일 단위, 기본 3) |
