# Stage 1: Build with uv (fast dependency resolver)
FROM python:3.13-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install basic deps needed for wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# Install dependencies first (cached layer)
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev --python 3.13 --quiet

# Copy actual source code
COPY ingestion ./ingestion

# Stage 2: Runtime image (small, non-root)
FROM python:3.13-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Create non-root user
RUN useradd -m -u 1001 appuser

WORKDIR /app

# Copy virtualenv + app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/ingestion /app/ingestion

# Use venv
ENV PATH="/app/.venv/bin:${PATH}"

USER 1001

# Cloud Run Job entrypoint
ENTRYPOINT ["python", "-m", "ingestion.app.main"]