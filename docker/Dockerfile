FROM python:3.11-slim AS base

ARG DEV=false

FROM base AS builder

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

# Install Poetry
RUN pip install poetry==2.1.3

# Copy only dependency metadata
COPY pyproject.toml poetry.lock ./

# Install main dependencies only
RUN if [ "$DEV" = "true" ]; then \
    poetry install --with dev --no-root && rm -rf $POETRY_CACHE_DIR; \
    else \
    poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR; \
    fi

FROM base AS runtime

# Install dependencies directly into system python to avoid virtualenv issues
COPY --from=builder /app/.venv/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/

ENV PATH="/usr/local/bin:${PATH}"

WORKDIR /app

# Copy source code and install the package
COPY src/ ./src/
COPY config/ ./config/
COPY pyproject.toml poetry.lock justfile README.md ./

# Install the package in editable mode so the climatesense_kg module is available
RUN pip install -e .

# Environment variables
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import climatesense_kg; print('OK')" || exit 1

# Default command
ENTRYPOINT [ "python", "-m", "climatesense_kg.cli" ]
CMD [ "--help" ]
