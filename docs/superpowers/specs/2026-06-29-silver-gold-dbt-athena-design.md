# silver/gold dbt-athena PoC — 설계 (Design)

- 작성일: 2026-06-29
- 상태: 승인됨 (구현 플랜 작성 대기)
- 관련 repo: `s3-pipeline-dbt` (본 repo) / 상위 소스: `../s3-pipeline` (raw→bronze)

## 1. 목적과 범위

medallion 레이크하우스의 **silver/gold 레이어**를, 이미 존재하는 **bronze Iceberg 테이블**
위에서 **dbt + Athena**로 구성한다. dbt는 **ECS Fargate 주기 배치**로 실행한다.

이 PoC의 1차 목표는 **파이프라인 배관(plumbing) 검증**이다:
`bronze → dbt-athena → silver → gold → ECS 주기실행`까지의 얇은 end-to-end 슬라이스 1개를
**실제 AWS Athena**에서 동작시키는 것이 핵심이다. 모델 수는 최소(silver 1 + gold 1)로 하되
컨테이너 빌드·배포·스케줄까지 포함한다.

### 범위에 포함

- dbt 프로젝트 스캐폴딩 (uv 기반, `dbt-athena` 어댑터)
- `bronze.metrics`를 dbt `source`로 선언
- `silver_metrics` (dedup + 타입정리, Iceberg incremental MERGE)
- `gold_metrics_daily` (일별 집계, Iceberg incremental MERGE)
- dbt 테스트 (키 unique/not_null, source freshness)
- ECS Fargate 배포 + EventBridge Scheduler 주기 실행 (`../s3-pipeline/deploy` 패턴 재사용)

### 범위에서 제외 (YAGNI)

- lookback 윈도우보다 더 늦게 도착한 지각 데이터의 백필 (별도 잡으로 추후)
- 다수의 silver/gold 모델, BI/대시보드 연동
- silver 외 추가 정합성/품질 프레임워크
- CI 파이프라인 (수동 빌드/푸시로 PoC 검증)

## 2. 핵심 결정

| 항목 | 결정 | 이유 |
|------|------|------|
| 어댑터 | 공식 `dbt-athena` | 유지보수 본류(구 dbt-athena-community 통합). uv로 관리 |
| 저장 포맷 | Iceberg (`table_type='iceberg'`) | bronze와 동일 생태계, ACID, 스키마 진화 |
| 적재 전략 | **incremental + `merge`** | dbt-athena Iceberg는 `append`/`merge`만 지원(`insert_overwrite` 미지원). dedup·지각데이터를 멱등 upsert |
| 스캔 제한 | `is_incremental()`에서 `dt >= max(dt) - lookback_days` | 매 실행 bronze/silver 스캔을 최근 파티션으로 제한 → 멱등 + 과금 안전 |
| lookback | `var('lookback_days')` 기본 **3** | 지각 데이터 흡수 vs 스캔비용 균형. env로 조정 |
| silver dedup 키 | `measurement, cdevice, pdevice, parameter, ts_ns` | bronze는 append라 같은 이벤트 재적재 가능. ts_ns(무손실 원본)로 유일 식별 |
| gold grain | `measurement, cdevice, pdevice, parameter, dt` | device2 + parameter + 일별 |
| 출력 스키마 | Glue DB `silver` / `gold` 분리 (폴더별 `+schema`) | 레이어 격리 |
| 실행 단위 | `dbt build` (run + test 한 번에) | ECS 엔트리포인트 단일화 |
| 스케줄 | EventBridge Scheduler, 기본 **일 1회** | 일별 집계라 일 1회면 충분. cron은 env 조정 |

## 3. bronze source 계약 (읽기 전용 — 재정의 금지)

`../s3-pipeline`이 생산하는 Glue/Iceberg 테이블 `bronze.metrics`. 스키마는 고정 계약이며
dbt `source`로만 선언한다(모델로 만들지 않는다). 파티션 키는 `dt`.

| 컬럼 | 타입 | 의미 |
|------|------|------|
| `cdevice` | string | tag (device) |
| `pdevice` | string | tag (device) |
| `parameter` | string | tag (metric 이름) |
| `value` | double | field (유일한 field) |
| `ts` | `timestamp(6) with time zone` | event time, μs, UTC |
| `ts_ns` | bigint | 원본 ns epoch (무손실) |
| `measurement` | string | line-protocol measurement |
| `dt` | date | event-date 파티션 키 (= `ts`의 UTC 날짜) |

> bronze는 현재 라이브 상태이며 실제 Athena에서 즉시 SELECT 가능(실측 E2E 가능).
> bronze는 at-ingested **append**이므로 중복/지각 데이터가 존재할 수 있고, **dedup은 silver의 책임**이다.

## 4. 컴포넌트 설계

### 4.1 데이터 흐름

```
bronze.metrics (source, 읽기 전용)
  └─ ref → silver_metrics       (Iceberg, incremental MERGE, dedup + 타입정리)
       └─ ref → gold_metrics_daily (Iceberg, incremental MERGE, 일별 집계)

`dbt build` 1회 = silver run → gold run → 전체 test
```

### 4.2 `models/silver/silver_metrics.sql`

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

- **배치 내 dedup**(row_number=1) 후 MERGE로 누적 upsert → 재적재/지각데이터 멱등 처리.
- 증분 조건이 매 실행 bronze 스캔을 최근 `lookback_days` 파티션으로 제한.
- **first run**: `is_incremental()`이 false라 bronze 전체를 1회 스캔(시드). 백필 범위를 끊고
  싶으면 `var('start_date')` 하한을 추가할 수 있다(선택, 기본 비활성).

### 4.3 `models/gold/gold_metrics_daily.sql`

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

- 집계 grain = MERGE 키 = `measurement, cdevice, pdevice, parameter, dt` → 최근 파티션 재집계 멱등 갱신.

### 4.4 source / 테스트 정의

- `models/bronze/_bronze__source.yml`: `source('bronze','metrics')` 선언 + freshness(`loaded_at_field: ts`).
- `models/silver/_silver__models.yml`: silver unique_key 조합 not_null, `value`/`ts` not_null,
  natural key 조합 unique(`dbt_utils.unique_combination_of_columns` 또는 동등 테스트).
- `models/gold/_gold__models.yml`: gold grain 조합 unique, `sample_count > 0`.

### 4.5 프로젝트 레이아웃

```
s3-pipeline-dbt/
  pyproject.toml              # uv: dbt-core, dbt-athena (+ dev: dbt-utils via packages.yml)
  dbt_project.yml            # name, profile, model paths, +schema(silver/gold), vars.lookback_days=3
  packages.yml               # dbt-utils (테스트용)
  profiles.yml              # env_var() 템플릿 (region/staging dir/data dir/workgroup/catalog)
  models/
    bronze/_bronze__source.yml
    silver/silver_metrics.sql
    silver/_silver__models.yml
    gold/gold_metrics_daily.sql
    gold/_gold__models.yml
  macros/
    generate_schema_name.sql # 커스텀 스키마를 prefix 없이 그대로 사용
  deploy/                    # ../s3-pipeline/deploy 패턴 재사용
    Dockerfile
    Dockerfile.dockerignore
    task-definition.json
    config.env.example       # 배포 도구용(계정/리전/role ARN/네트워크)
    task.env.example         # 컨테이너 런타임 env (S3에 업로드)
    iam/                     # ecs-tasks-trust, task-permissions(Athena+Glue+S3), exec, scheduler
    scripts/                 # build_and_push, register_task, run_once, create_schedule, _common
```

### 4.6 프로필 / 환경 주입

`profiles.yml`은 `env_var()`로 값을 채워 git에 안전하게 커밋하고, 실제 값은 ECS의 S3 env
파일(`task.env`) 또는 로컬 env로 주입한다(`../s3-pipeline`의 config/task env 분리와 동일).

```yaml
# profiles.yml (커밋 가능 — 값은 env에서)
s3_pipeline_dbt:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  outputs:
    dev:
      type: athena
      region_name: "{{ env_var('AWS_REGION') }}"
      database: awsdatacatalog            # Glue catalog
      schema: "{{ env_var('DBT_SCHEMA', 'silver') }}"  # 기본 출력 스키마
      s3_staging_dir: "{{ env_var('ATHENA_STAGING_DIR') }}"
      s3_data_dir: "{{ env_var('ATHENA_DATA_DIR') }}"
      work_group: "{{ env_var('ATHENA_WORKGROUP') }}"
      threads: 4
```

silver/gold 분리는 `dbt_project.yml`의 폴더별 `+schema: silver|gold` + `macros/generate_schema_name.sql`
(커스텀 스키마를 prefix 없이 그대로 사용)로 구현한다.

## 5. 배포 / 스케줄 (`../s3-pipeline/deploy` 재사용)

- **Dockerfile**: `ghcr.io/astral-sh/uv:python3.12-*` 베이스, `uv sync --frozen --no-dev`,
  소스 복사 후 `dbt deps`. ENTRYPOINT `dbt build --target prod`. `DBT_PROFILES_DIR=/app`.
- **task-definition.json**: Fargate, `environmentFiles`로 S3 `task.env` 주입
  (AWS_REGION, ATHENA_STAGING_DIR, ATHENA_DATA_DIR, ATHENA_WORKGROUP, DBT_TARGET=prod, lookback_days 등).
  awslogs로 CloudWatch 로그.
- **IAM task-role**: Athena(StartQueryExecution/Get*), Glue(Get/Update/Create Table·Database),
  S3(staging dir RW + silver/gold data dir RW + bronze data dir R).
- **EventBridge Scheduler**: 기본 일 1회 cron으로 ECS RunTask 트리거(scheduler-role 사용).

## 6. 검증 (실측 E2E)

배관 검증이 1차 목표이므로 다음을 성공 기준으로 한다:

1. `uv run dbt debug` → Athena 연결/카탈로그/staging 접근 성공.
2. `uv run dbt build --select silver_metrics` → Glue `silver.silver_metrics` 생성, Athena에서
   행수·dedup(키 중복 0) 확인.
3. `uv run dbt build --select gold_metrics_daily` → `gold.gold_metrics_daily` 생성, 일별 집계값 확인.
4. 컨테이너 이미지 빌드 → `run_once`로 ECS 1회 실행 → CloudWatch 로그에서 `dbt build` 성공.
5. EventBridge Scheduler 등록 → 다음 주기 실행이 자동 트리거되어 멱등 갱신됨을 확인.

## 7. 리스크 / 미해결

- **first-run 전체 스캔**: silver 최초 빌드가 bronze 전체를 스캔한다. PoC 데이터량에선 수용
  가능하나, 크면 `var('start_date')` 하한으로 범위를 끊는다.
- **lookback 밖 지각 데이터**: 3일보다 늦게 도착하면 누락된다 → 백필 잡(범위 외)으로 처리.
- **ts 타입 정합**: bronze `ts`가 `timestamp(6) with time zone`이어야 한다. silver의 `date_add`/
  `max(dt)` 비교는 `dt`(date) 기준이라 영향 없음.
- **MERGE 비용**: merge는 파티션 재처리보다 무거울 수 있으나 lookback 스캔 제한으로 PoC 범위에선 허용.
