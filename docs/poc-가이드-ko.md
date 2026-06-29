# s3-pipeline-dbt PoC 진행 가이드 (한국어)

> 내일 이어서 PoC를 진행하기 위한 한국어 핸드오프 문서입니다. 상세 영문 문서로의 포인터를
> 모아둔 "출발점" 역할입니다. 깊은 내용은 각 문서를 참고하세요.

## 1. 이 프로젝트가 하는 일

`../s3-pipeline`이 만든 **bronze Iceberg 테이블(`bronze.metrics`)** 위에, **dbt + Athena**로
**silver / gold** 레이어를 구성합니다. dbt는 **ECS Fargate 배치**로 주기 실행(기본 일 1회)합니다.
목표는 **실제 AWS Athena 대상 PoC**(배관 검증)입니다.

```
MSK → Firehose → S3 raw → S3 bronze(Iceberg)   ← ../s3-pipeline (완료)
                              → dbt-athena → silver → gold   ← 이 repo
```

- 설계 스펙: [superpowers/specs/2026-06-29-silver-gold-dbt-athena-design.md](superpowers/specs/2026-06-29-silver-gold-dbt-athena-design.md)
- 구현 플랜: [superpowers/plans/2026-06-29-silver-gold-dbt-athena.md](superpowers/plans/2026-06-29-silver-gold-dbt-athena.md)
- 모델 가이드: [../models/README.md](../models/README.md)
- 배포·AWS 검증 런북: [../deploy/README.md](../deploy/README.md)
- repo 운영 규칙: [../CLAUDE.md](../CLAUDE.md)

## 2. 현재 상태 (오늘까지)

- **브랜치**: `feat/silver-gold-dbt-athena` (master에서 분기, 로컬 커밋만. 리모트 push는 내일)
- **완료 (코드/구성 + 오프라인 검증)**:
  - uv + dbt-athena 프로젝트 스캐폴딩 (Python 3.12, dbt-core 1.11 / dbt-athena 1.10)
  - Athena profile(`profiles.yml`, env 기반) + `generate_schema_name` 매크로(silver/gold/example DB 분리)
  - bronze source 선언
  - `silver_metrics`(dedup), `gold_metrics_daily`(일별 집계) — Iceberg incremental **merge**
  - 예시: `silver_metrics_aa010`, `gold_metrics_daily_aa010` (measurement='AA010' 슬라이스)
  - 컨테이너 이미지(Dockerfile), ECS task-def, IAM 정책, 배포 스크립트, 런북
  - dbt `parse`/`list`/`compile` 오프라인 통과, warning-free
- **미완료 (실 AWS 필요 → 내일)**: `dbt debug`/`dbt build` 실측, 컨테이너 빌드/푸시, ECS 배포, 스케줄.
  전부 [../deploy/README.md](../deploy/README.md)에 단계별로 정리됨.

## 3. 내일 진행 순서 (요약)

상세는 [../deploy/README.md](../deploy/README.md). 큰 흐름만:

1. **사전 준비 확인** (런북 §0): AWS 자격증명, `bronze.metrics` 데이터, Athena workgroup,
   S3 staging/data prefix, 도구(`uv`/`docker`/`aws`/`envsubst`).
2. **로컬 실측** (런북 §1): env export → `uv run dbt debug` → `dbt build --select silver_metrics`
   → dedup 쿼리(`dup_rows=0`) → `dbt build --select gold_metrics_daily` → `uv run dbt build`(전체).
3. **배포** (런북 §2): `config.env`/`task.env` 작성 → IAM role 생성 → `build_and_push.sh` →
   `register_task.sh` → `run_once.sh`(+CloudWatch 로그) → `create_schedule.sh`.
4. **리모트 정리**: 검증 OK면 `git push -u origin feat/silver-gold-dbt-athena` 후 PR 생성
   (또는 master 병합).

## 4. 빠른 재개 체크리스트 (로컬 실측 직전)

```bash
cd /Users/seominji/work/s3-pipeline-dbt
git checkout feat/silver-gold-dbt-athena

# 실제 값으로 채우기
export AWS_REGION=<region>
export ATHENA_STAGING_DIR=s3://<bucket>/_athena/staging/
export ATHENA_DATA_DIR=s3://<bucket>/silver/
export ATHENA_WORKGROUP=<workgroup>
export DBT_SCHEMA=silver
export DBT_TARGET=dev

uv run dbt deps      # dbt_utils 설치
uv run dbt debug     # Athena 연결 확인 → "Connection test: OK"
uv run dbt build     # silver → gold → example + 테스트
```

> ⚠️ **첫 `dbt build`는 bronze 전체를 1회 스캔**(silver 시드)합니다. 데이터가 크면 비용 주의 —
> 런북 Appendix A의 `start_date` 하한 참고. 이후 실행은 `lookback_days`(기본 3일)로 스캔 범위가 제한됩니다.

## 5. 핵심 설계 결정 (기억해둘 것)

| 항목 | 결정 | 이유 |
|------|------|------|
| 적재 전략 | Iceberg incremental **merge** | dbt-athena Iceberg는 append/merge만 지원(insert_overwrite ✕). dedup·지각데이터 멱등 upsert |
| 스캔 제한 | `dt >= max(dt) - lookback_days` | `dt`(identity 파티션)로 프루닝 → 비용 제한 + 멱등 |
| `dt` 사용 이유 | 물리 파티션 컬럼(=UTC date of ts, 1회 계산) | `day(ts)`는 매 쿼리 재계산·프루닝 불가·타임존 위험. ([../models/README.md](../models/README.md) 참고) |
| silver dedup 키 | measurement+cdevice+pdevice+parameter+ts_ns | bronze는 append라 재적재 가능 → ts_ns(무손실)로 유일 식별 |
| gold grain | measurement+cdevice+pdevice+parameter+dt | device2+parameter 일별 |
| 출력 스키마 | 폴더별 `+schema` → Glue DB `silver`/`gold`/`example` | 레이어 격리 |
| 실행 단위 | `dbt build`(run+test) = ECS 엔트리포인트 | 단일화 |

## 6. 자주 쓰는 명령어

```bash
uv run dbt build --select silver_metrics          # 특정 모델만
uv run dbt build --select silver_metrics+         # 그 모델 + 하류 전부
uv run dbt test  --select gold_metrics_daily      # 테스트만
uv run dbt list  --select example_measurement     # 노드/테스트 확인
uv run dbt compile --select <model>               # 생성 SQL 확인
uv run dbt build --vars '{lookback_days: 7}'      # lookback 조정
```

## 7. 다음 단계 / 확장 아이디어 (PoC 이후)

- lookback 밖 **지각 데이터 백필** 잡(증분과 분리된 별도 관심사 — 스펙 §7)
- silver를 행 단위 정합이 중요해지면 `merge` 유지하되 SCD/품질 테스트 강화
- 다른 measurement 슬라이스: `models/example_measurement/` 복사 후 `where measurement = '...'` 변경
- gold 집계 grain 확장(시간별 등), BI 연동
- CI(현재 PoC는 수동 빌드/푸시)
