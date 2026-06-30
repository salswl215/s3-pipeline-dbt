"""
run_dbt.py — ECS Fargate dbt 실행 래퍼

stdout 로그는 awslogs 드라이버가 CloudWatch로 그대로 캡처한다. 이 래퍼가 더하는 것:
  - dbt build 후 target/run_results.json을 파싱해 **기계가 읽을 수 있는** 요약을 남김
    ([DBT_SUMMARY] / [DBT_FAIL]) → CloudWatch metric filter·알림에서 성패를 잡아낼 수 있음
  - 실패 시 `dbt retry`로 실패 노드만 재실행 (일시적 오류 대응)
  - 모델 실패(retry 의미 있음) vs 프로세스 오류(retry 스킵) 구분

dbt deps는 이미지 빌드 시 설치되므로(Dockerfile) 런타임에서 다시 실행하지 않는다.
색상은 NO_COLOR=1(Dockerfile) 환경변수로 끈다.

환경변수:
  DBT_COMMAND        실행할 dbt 명령 (기본: "dbt build --target prod")
  DBT_VARS           dbt --vars 로 전달할 값 (선택, 예: '{start_date: "2026-06-25"}')
  DBT_RETRY_ENABLED  실패 시 dbt retry 여부 (기본: true)
  DBT_PROJECT_DIR    dbt 프로젝트 경로 (기본: /app)
"""

import json
import logging
import os
import shlex
import subprocess
import sys
from collections import Counter
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,  # dbt 출력(stdout)과 같은 스트림 → CloudWatch 순서 일관
)
log = logging.getLogger("run_dbt")

PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/app"))
RUN_RESULTS_PATH = PROJECT_DIR / "target" / "run_results.json"

FAILURE_STATUSES = {"error", "fail"}
SUCCESS_STATUSES = {"success", "pass"}


def run(argv: list[str]) -> int:
    """dbt 명령을 셸 없이(shell=False) 실행 — DBT_VARS 쿼팅/인젝션 회피."""
    log.info("$ %s", " ".join(argv))
    return subprocess.run(argv, cwd=PROJECT_DIR, check=False).returncode  # returncode 직접 처리


def _vars_args() -> list[str]:
    """DBT_VARS가 있으면 --vars 인자로 (리스트 인자라 셸 쿼팅 불필요)."""
    dbt_vars = os.getenv("DBT_VARS", "").strip()
    return ["--vars", dbt_vars] if dbt_vars else []


def build_argv() -> list[str]:
    """DBT_COMMAND(+ 선택적 DBT_VARS)를 안전한 인자 리스트로 조립."""
    return shlex.split(os.getenv("DBT_COMMAND", "dbt build --target prod")) + _vars_args()


def parse_run_results() -> list[dict]:
    """target/run_results.json의 results를 반환. 없거나 손상 시 빈 리스트."""
    if not RUN_RESULTS_PATH.exists():
        log.warning("run_results.json not found (%s)", RUN_RESULTS_PATH)
        return []
    try:
        with open(RUN_RESULTS_PATH, encoding="utf-8") as f:
            return json.load(f).get("results") or []  # "results": null 도 방어
    except (json.JSONDecodeError, OSError) as e:
        log.warning("run_results.json 파싱 실패: %s", e)
        return []


def summarize(label: str, results: list[dict]) -> list[dict]:
    """status별 집계를 [DBT_SUMMARY]로 남기고 실패 노드 리스트를 반환."""
    counts = Counter(r.get("status", "unknown") for r in results)
    failures = [r for r in results if r.get("status") in FAILURE_STATUSES]
    log.info(
        "[DBT_SUMMARY] label=%s total=%d ok=%d fail=%d skip=%d warn=%d",
        label,
        len(results),
        sum(counts[s] for s in SUCCESS_STATUSES),
        len(failures),
        counts.get("skipped", 0),
        counts.get("warn", 0),
    )
    for f in failures:
        # 한 줄로 평탄화 → [DBT_FAIL]이 단일 로그 이벤트로 남아 metric filter가 파싱 가능
        msg = " ".join((f.get("message") or "").split())[:200]
        log.error("[DBT_FAIL] %s — %s", f.get("unique_id", "unknown"), msg)
    return failures


def main() -> int:
    argv = build_argv()
    retry_enabled = os.getenv("DBT_RETRY_ENABLED", "true").lower() == "true"
    log.info("run_dbt 시작 | 명령어: %s | retry: %s", " ".join(argv), retry_enabled)

    # 1차 실행 (dbt deps는 이미지 빌드 시 설치 완료)
    exit_code = run(argv)
    failures = summarize("first", parse_run_results())

    # 2차: 실패 노드만 dbt retry로 재실행 (모델 실패일 때만 의미 있음)
    if exit_code != 0 and retry_enabled:
        if failures:
            log.warning("%d개 노드 실패 — dbt retry 재시도", len(failures))
            # target은 retry가 run_results.json에서 재사용. vars만 명시(버전 무관 안전).
            exit_code = run(["dbt", "retry", *_vars_args()])
            summarize("retry", parse_run_results())
        else:
            log.error("dbt 프로세스 오류 (모델 실패 아님) — retry 스킵")

    if exit_code != 0:
        log.error("최종 실패 — ECS 태스크 실패로 기록 (exit=%d)", exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
