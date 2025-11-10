FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv first
RUN pip install uv

# Copy dependency files ONLY
COPY pyproject.toml uv.lock ./

# Install dependencies (cached layer)
RUN uv sync --frozen --no-dev

# Copy application code ONLY (no data files due to .dockerignore)
COPY uk_address_matcher/ ./uk_address_matcher/
COPY container_main.py ./
COPY process_canonical.py ./
COPY process_messy.py ./

# Create non-root user
RUN useradd -m -u 1000 matcher && \
    chown -R matcher:matcher /app && \
    mkdir -p /tmp/duckdb && \
    chown -R matcher:matcher /tmp/duckdb

USER matcher

# Default environment variables (can be overridden in ECS task)
ENV DUCKDB_MEMORY_LIMIT=12GB \
    DUCKDB_THREADS=0 \
    DUCKDB_TEMP_DIR_SIZE=100GB \
    AWS_DEFAULT_REGION=eu-west-2 \
    S3_BUCKET=uk-address-matcher-data \
    PROCESSING_MODE=messy

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import uk_address_matcher; import boto3; print('OK')" || exit 1

# Entry point
CMD ["uv", "run", "python", "container_main.py"]