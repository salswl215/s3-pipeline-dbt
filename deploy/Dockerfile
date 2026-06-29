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
COPY --chown=appuser:appuser dbt_project.yml packages.yml package-lock.yml profiles.yml ./
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
