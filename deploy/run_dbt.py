"""
run_dbt.py — ECS Fargate dbt 실행 래퍼

stdout 로그는 awslogs 드라이버가 CloudWatch로 그대로 캡처한다. 이 래퍼가 더하는 것:
  - dbt build 후 target/run_results.json을 파싱해 **기계가 읽을 수 있는** 요약을 남김
    ([DBT_SUMMARY] / [DBT_FAIL]) → CloudWatch metric filter·알림에서 성패를 잡아낼 수 있음
  - 실패 시 `dbt retry`로 실패 노드만 재실행 (일시적 오류 대응)
  - 모델 실패(retry 의미 있음) vs 프로세스 오류(retry 스킵) 구분

dbt deps는 이미지 빌드 시 설치되므로(Dockerfile) 런타임에서 다시 실행하지 않는다.

환경변수:
  DBT_COMMAND        실행할 dbt 명령 (기본: "dbt build --target prod")
  DBT_VARS           dbt --vars 로 전달할 값 (선택, 예: '{start_date: "2026-06-25"}')
  DBT_RETRY_ENABLED  실패 시 dbt retry 여부 (기본: true)
  DBT_PROJECT_DIR    dbt 프로젝트 경로 (기본: /app)
"""

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/app"))
RUN_RESULTS_PATH = DBT_PROJECT_DIR / "target" / "run_results.json"

FAILURE_STATUSES = {"error", "fail"}


def run(command: str) -> int:
    log.info("$ %s", command)
    return subprocess.run(command, shell=True, cwd=DBT_PROJECT_DIR).returncode


def parse_run_results() -> tuple[list[dict], list[dict]]:
    if not RUN_RESULTS_PATH.exists():
        log.warning("run_results.json not found (%s)", RUN_RESULTS_PATH)
        return [], []
    with open(RUN_RESULTS_PATH) as f:
        data = json.load(f)
    results = data.get("results", [])
    failures = [r for r in results if r.get("status") in FAILURE_STATUSES]
    return failures, results


def summarize(label: str, failures: list[dict], results: list[dict]) -> None:
    total = len(results)
    log.info(
        "[DBT_SUMMARY] label=%s total=%d success=%d fail=%d",
        label, total, total - len(failures), len(failures),
    )
    for f in failures:
        log.error("[DBT_FAIL] %s — %s", f.get("unique_id", "unknown"), (f.get("message") or "")[:200])


def main() -> None:
    dbt_command = os.getenv("DBT_COMMAND", "dbt build --target prod")
    dbt_vars = os.getenv("DBT_VARS", "")
    retry_enabled = os.getenv("DBT_RETRY_ENABLED", "true").lower() == "true"

    # color is disabled via the NO_COLOR=1 env var (set in the Dockerfile); dbt 1.11 has no
    # --no-color flag (it's --no-use-colors), and the env var covers it without a flag.
    if dbt_vars:
        dbt_command = f"{dbt_command} --vars '{dbt_vars}'"

    log.info("run_dbt 시작 | 명령어: %s | retry: %s", dbt_command, retry_enabled)

    # 1차 실행 (dbt deps는 이미지 빌드 시 설치 완료)
    exit_code = run(dbt_command)
    failures, results = parse_run_results()
    summarize("first", failures, results)

    # 2차: 실패 노드만 dbt retry로 재실행
    if exit_code != 0 and retry_enabled:
        if failures:
            log.warning("%d개 노드 실패 — dbt retry 재시도", len(failures))
            exit_code = run("dbt retry")
            failures, results = parse_run_results()
            summarize("retry", failures, results)
        else:
            log.error("dbt 프로세스 오류 (모델 실패 아님) — retry 스킵")

    if exit_code != 0:
        log.error("최종 실패 — ECS 태스크 실패로 기록 (exit=%d)", exit_code)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
