# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

FROM python:3.12-slim-bookworm AS runtime
# patch known OS-level CVEs in the base image
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 10001 appuser
WORKDIR /app
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
# dbt project files
COPY --chown=appuser:appuser dbt_project.yml packages.yml package-lock.yml profiles.yml ./
COPY --chown=appuser:appuser models ./models
COPY --chown=appuser:appuser macros ./macros
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    NO_COLOR=1 \
    DBT_PROFILES_DIR=/app
# install dbt packages (dbt_utils) into the image
RUN dbt deps
# dbt writes logs/ and target/ under /app at runtime. Pre-create just those two dirs owned
# by the runtime user (dbt deps above ran as root). Narrow chown — NOT `-R` on /app — so the
# large .venv isn't rewritten into a new layer, keeping the image small.
RUN rm -rf /app/logs \
 && mkdir -p /app/logs /app/target \
 && chown appuser:appuser /app/logs /app/target
# dbt run wrapper: structured run_results summary + dbt retry on failure (copied late to
# keep the dbt deps / venv layers cached when only the wrapper changes)
COPY --chown=appuser:appuser deploy/run_dbt.py ./deploy/run_dbt.py
USER appuser
ENTRYPOINT ["python", "/app/deploy/run_dbt.py"]
