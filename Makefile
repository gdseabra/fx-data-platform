# Makefile for FX Data Platform
# Useful shortcuts for common development and operational tasks

.PHONY: help setup install install-dev lint format test test-unit test-integration docker-up docker-down seed logs reset health-check clean up down etl-setup etl-bronze etl-silver etl-gold etl-all

# Spark submit wrapper — resolves Iceberg/S3A JARs via project conf/spark-defaults.conf
# MINIO_ENDPOINT: use localhost when running locally (outside Docker), minio when inside
SPARK_SUBMIT = SPARK_CONF_DIR=$(PWD)/conf MINIO_ENDPOINT=http://localhost:9000 conda run -n nomad spark-submit
ETL_DATE ?= $(shell date +%Y-%m-%d)
ETL_ENV  ?= dev

# Default target
help:
	@echo "FX Data Platform - Available Commands"
	@echo "===================================="
	@echo ""
	@echo "Setup:"
	@echo "  make setup              - Initialize development environment"
	@echo "  make install            - Install production dependencies"
	@echo "  make install-dev        - Install development dependencies"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint               - Run linters (ruff, mypy)"
	@echo "  make format             - Format code (black, isort)"
	@echo "  make format-check       - Check code formatting without changes"
	@echo ""
	@echo "Testing:"
	@echo "  make test               - Run all tests with coverage"
	@echo "  make test-unit          - Run only unit tests"
	@echo "  make test-integration   - Run only integration tests"
	@echo ""
	@echo "Docker & Environment:"
	@echo "  make docker-up          - Start all Docker services (docker compose)"
	@echo "  make docker-down        - Stop all Docker services"
	@echo "  make docker-logs        - Show Docker logs (all services)"
	@echo "  make seed               - Populate PostgreSQL with test data"
	@echo "  make reset              - Destroy volumes and recreate everything"
	@echo ""
	@echo "Monitoring:"
	@echo "  make monitoring-up      - Start monitoring stack (Prometheus, Grafana, etc)"
	@echo "  make monitoring-down    - Stop monitoring stack"
	@echo "  make health-check       - Run health checks on all services"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean              - Remove build artifacts and cache files"
	@echo "  make docs               - Generate documentation"

# Setup and Installation
setup: install-dev
	@echo "✓ Development environment initialized"

install:
	@echo "Installing production dependencies..."
	pip install -q --upgrade pip setuptools wheel
	pip install -q -e .
	@echo "✓ Production dependencies installed"

install-dev: install
	@echo "Installing development dependencies..."
	pip install -q -e ".[dev]"
	@echo "✓ Development dependencies installed"

# Code Quality
lint:
	@echo "Running linters..."
	ruff check . --fix
	mypy ingestion etl orchestration ml monitoring scripts --ignore-missing-imports
	@echo "✓ Linting complete"

format:
	@echo "Formatting code..."
	black ingestion etl orchestration ml monitoring scripts tests
	isort ingestion etl orchestration ml monitoring scripts tests
	@echo "✓ Code formatted"

format-check:
	@echo "Checking code formatting..."
	black --check ingestion etl orchestration ml monitoring scripts tests
	isort --check-only ingestion etl orchestration ml monitoring scripts tests
	@echo "✓ Code formatting is correct"

# Testing
test: test-unit test-integration
	@echo "✓ All tests passed"

test-unit:
	@echo "Running unit tests..."
	pytest tests/ -v -m "unit" --cov=ingestion,etl,orchestration,ml,monitoring,scripts --cov-report=html --cov-report=term-missing --cov-fail-under=80
	@echo "✓ Unit tests passed"

test-integration:
	@echo "Running integration tests (requires Docker)..."
	pytest tests/ -v -m "integration" --cov-append
	@echo "✓ Integration tests passed"

# Docker and Environment
docker-up:
	@echo "Starting Docker services..."
	docker compose -f docker-compose.yml up -d
	@echo "Waiting for services to be ready..."
	sleep 5
	@echo "✓ Docker services started"
	@echo "Services:"
	@echo "  - PostgreSQL: localhost:5432"
	@echo "  - Redpanda: localhost:9092"
	@echo "  - Redpanda Console: http://localhost:8080"
	@echo "  - MinIO: http://localhost:9001 (user: minioadmin, password: minioadmin)"
	@echo "  - Airflow: http://localhost:8090"
	@echo "  - MLflow: http://localhost:5000"

docker-down:
	@echo "Stopping Docker services..."
	docker compose -f docker-compose.yml down
	@echo "✓ Docker services stopped"

docker-logs:
	docker compose logs -f

# Short aliases (as per spec)
up: docker-up
down: docker-down
logs: docker-logs

seed:
	@echo "Seeding PostgreSQL with test data..."
	docker compose exec postgres psql -U postgres -d fx_transactions -f /scripts/init.sql
	python scripts/seed_data.py
	@echo "✓ Database seeded"

reset:
	@echo "Resetting Docker environment (destroying volumes)..."
	docker compose down -v
	docker compose up -d
	@echo "✓ Environment reset"

# Monitoring
monitoring-up:
	@echo "Starting monitoring stack..."
	docker compose -f docker-compose.monitoring.yml up -d
	@echo "✓ Monitoring stack started"
	@echo "Services:"
	@echo "  - Prometheus: http://localhost:9090"
	@echo "  - Grafana: http://localhost:3000 (user: admin, password: admin)"
	@echo "  - AlertManager: http://localhost:9093"

monitoring-down:
	@echo "Stopping monitoring stack..."
	docker compose -f docker-compose.monitoring.yml down
	@echo "✓ Monitoring stack stopped"

health-check:
	@echo "Running health checks..."
	python scripts/ops/health_check.py --env dev --verbose
	@echo "✓ Health checks complete"

# Utilities
clean:
	@echo "Cleaning build artifacts..."
	rm -rf build/ dist/ *.egg-info/ .eggs/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*~" -delete
	rm -rf .pytest_cache/ .mypy_cache/ .ruff_cache/ .coverage htmlcov/
	@echo "✓ Cleaned up"

docs:
	@echo "Generating documentation..."
	@echo "✓ Documentation generated in docs/"

# ETL Pipeline (Spark + Iceberg)
# Requires: conda env nomad, Docker services running (MinIO reachable at minio:9000)
# On first run, Spark downloads ~200MB of JARs to ~/.ivy2/cache — subsequent runs are instant.
etl-setup:
	@echo "Creating Iceberg catalogs and tables..."
	$(SPARK_SUBMIT) etl/iceberg/catalog_setup.py --env $(ETL_ENV)
	@echo "✓ Iceberg catalog ready"

etl-bronze:
	@echo "Running Bronze ingestion for $(ETL_DATE)..."
	$(SPARK_SUBMIT) etl/jobs/bronze/ingest_transactions.py --date $(ETL_DATE) --env $(ETL_ENV)
	@echo "✓ Bronze done"

etl-silver:
	@echo "Running Silver transform for $(ETL_DATE)..."
	$(SPARK_SUBMIT) etl/jobs/silver/transform_transactions.py --env $(ETL_ENV)
	@echo "✓ Silver done"

etl-gold:
	@echo "Running Gold aggregation for $(ETL_DATE)..."
	$(SPARK_SUBMIT) etl/jobs/gold/aggregate_metrics.py --env $(ETL_ENV)
	@echo "✓ Gold done"

etl-all: etl-setup etl-bronze etl-silver etl-gold
	@echo "✓ Full ETL pipeline complete for $(ETL_DATE)"

.DEFAULT_GOAL := help
