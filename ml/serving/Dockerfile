# ml/serving/Dockerfile
# Multi-stage build for the FX Anomaly Detection inference service.
#
# Stage 1 (builder) — installs Python deps into a venv.
# Stage 2 (runtime) — copies only the venv + source; no build tools.
#
# Build:
#   docker build -t fx-inference:latest -f ml/serving/Dockerfile .
#
# Run:
#   docker run -p 8000:8000 \
#     -e MLFLOW_TRACKING_URI=http://mlflow:5000 \
#     -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
#     fx-inference:latest

# ─── Stage 1: builder ────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System deps needed to compile some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Create isolated venv
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install only the serving runtime dependencies (not Spark, Airflow, etc.)
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip wheel && \
    pip install --no-cache-dir \
        fastapi>=0.104.0 \
        uvicorn[standard]>=0.24.0 \
        pydantic>=2.0.0 \
        mlflow>=2.10.0 \
        scikit-learn>=1.4.0 \
        numpy>=1.24.0 \
        pandas>=2.0.0 \
        feast>=0.38.0 \
        confluent-kafka>=2.3.0 \
        prometheus-client>=0.19.0 \
        structlog>=23.3.0 \
        python-dotenv>=1.0.0 \
        boto3>=1.34.0 \
        httpx>=0.25.0


# ─── Stage 2: runtime ────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Non-root user for security
RUN groupadd --system appuser && useradd --system --gid appuser appuser

WORKDIR /app

# Copy venv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application source (only what serving needs)
COPY ml/__init__.py ml/__init__.py
COPY ml/serving/ ml/serving/
COPY ml/feature_store/ ml/feature_store/
COPY ml/config/ ml/config/

# Create writable data dir for Feast local registry (dev mode)
RUN mkdir -p data/feature_store && chown -R appuser:appuser /app

USER appuser

# Uvicorn will bind to this port
EXPOSE 8000

# Healthcheck — hits /health every 30s; 3 failures = unhealthy
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import httpx; r=httpx.get('http://localhost:8000/health', timeout=4); r.raise_for_status()"

# Default entrypoint
CMD ["uvicorn", "ml.serving.inference_service:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "2", \
     "--log-level", "info"]
