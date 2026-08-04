FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=0

WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev
COPY pyproject.toml uv.lock README.md /app/
COPY src/ /app/src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

FROM python:3.11-slim-bookworm AS runtime

WORKDIR /app

COPY --from=builder --chown=app:app /app/.venv /app/.venv

COPY config/ ./config/

ENV PATH="/app/.venv/bin:$PATH"

# Environment variables
ENV PYTHONUNBUFFERED=1

# Default command
ENTRYPOINT [ "climatesense-kg" ]
CMD [ "--help" ]
