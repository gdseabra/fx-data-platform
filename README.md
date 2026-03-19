# FX Data Platform 🚀

[![Python CI](https://github.com/YOUR_ORG/fx-data-platform/actions/workflows/python-ci.yml/badge.svg)](https://github.com/YOUR_ORG/fx-data-platform/actions/workflows/python-ci.yml)
[![Spark Tests](https://github.com/YOUR_ORG/fx-data-platform/actions/workflows/spark-tests.yml/badge.svg)](https://github.com/YOUR_ORG/fx-data-platform/actions/workflows/spark-tests.yml)
[![Terraform CI](https://github.com/YOUR_ORG/fx-data-platform/actions/workflows/terraform-ci.yml/badge.svg)](https://github.com/YOUR_ORG/fx-data-platform/actions/workflows/terraform-ci.yml)
[![Terraform CD](https://github.com/YOUR_ORG/fx-data-platform/actions/workflows/terraform-cd.yml/badge.svg)](https://github.com/YOUR_ORG/fx-data-platform/actions/workflows/terraform-cd.yml)

A production-grade real-time data engineering platform for processing FX (foreign exchange) transactions with ML-based anomaly detection. This is a modern data stack demonstrating best practices in data engineering, MLOps, and cloud infrastructure.

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Deployment](#deployment)
- [Documentation](#documentation)
- [Contributing](#contributing)

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FX DATA PLATFORM ARCHITECTURE                       │
└─────────────────────────────────────────────────────────────────────────────┘

                    ┌──────────────────────────────────┐
                    │   REAL-TIME DATA INGESTION       │
                    │  (PostgreSQL → Redpanda → MinIO) │
                    └────────────────┬─────────────────┘
                                     │
            ┌────────────────────────┴────────────────────────┐
            │                                                 │
    ┌───────▼──────────┐                              ┌──────▼──────────┐
    │   STREAMING      │                              │   BATCH LAKE    │
    │  (Debezium CDC)  │                              │   (Raw/Bronze)  │
    │   Redpanda       │                              │     Parquet     │
    └────────┬─────────┘                              └────────┬────────┘
             │                                                 │
             └─────────────────────┬───────────────────────────┘
                                   │
                    ┌──────────────▼─────────────┐
                    │    ETL TRANSFORMATIONS     │
                    │  (PySpark + Iceberg)       │
                    │  Bronze → Silver → Gold    │
                    └──────────────┬─────────────┘
                                   │
            ┌──────────────────────┴──────────────────────┐
            │                                             │
      ┌─────▼─────────┐                           ┌──────▼──────────┐
      │   ANALYTICS   │                           │   ML PIPELINE   │
      │   Lake (Gold) │                           │  Feature Store  │
      │   Tables      │                           │   MLflow        │
      └───────────────┘                           │   Training      │
                                                   └────────┬────────┘
                                                            │
                                                   ┌────────▼─────────┐
                                                   │ REAL-TIME MODEL  │
                                                   │ Serving (FastAPI)│
                                                   │ Anomaly Scoring  │
                                                   └──────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  SUPPORTING INFRASTRUCTURE                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  • Orchestration: Apache Airflow (DAGs, scheduling, monitoring)             │
│  • IaC: Terraform (AWS resources: S3, Glue, IAM, EMR)                       │
│  • CI/CD: GitHub Actions (automated testing, linting, deployment)           │
│  • Monitoring: Prometheus + Grafana (metrics, dashboards, alerting)         │
│  • Logging: Structured logs → CloudWatch (audit trail)                      │
└─────────────────────────────────────────────────────────────────────────────┘

DATA FLOW SUMMARY:
1. Transactions are captured from PostgreSQL via Debezium CDC
2. Events stream through Redpanda (Kafka-compatible)
3. Redpanda Connect sinks data to MinIO/S3 (RAW layer)
4. Spark jobs transform: RAW → Bronze → Silver → Gold
5. Feature Store (Feast) materializes features for ML
6. ML models trained, validated, and deployed via MLflow
7. Real-time inference API scores transactions for anomalies
8. All metrics and alerts visualized in Grafana dashboards
```

## 📁 Project Structure

```
fx-data-platform/
│
├── infra/terraform/                 # Infrastructure as Code (AWS)
│   ├── modules/
│   │   ├── s3/                      # S3 Data Lake buckets with lifecycle policies
│   │   ├── iam/                     # IAM roles and policies
│   │   └── glue/                    # AWS Glue Catalog and Crawlers
│   ├── environments/
│   │   └── dev/                     # Development environment configuration
│   ├── main.tf                      # Root module orchestration
│   ├── variables.tf
│   └── outputs.tf
│
├── ingestion/                       # Real-time Data Ingestion
│   ├── producer/                    # Kafka/Redpanda producers
│   │   ├── streaming_producer.py    # Main transaction producer
│   │   ├── exchange_rate_producer.py
│   │   └── config.py
│   ├── consumer/                    # Kafka consumers
│   │   └── health_check_consumer.py
│   ├── debezium/                    # CDC configuration
│   │   └── connector-config.json
│   ├── redpanda-connect/            # Stream processing pipelines
│   │   └── pipeline.yaml
│   └── schemas/                     # JSON schemas for validation
│       ├── transaction_event.json
│       └── user_event.json
│
├── etl/                             # Spark ETL Jobs
│   ├── jobs/
│   │   ├── bronze/                  # Raw → Bronze (dedup, schema)
│   │   │   └── ingest_transactions.py
│   │   ├── silver/                  # Bronze → Silver (clean, enrich)
│   │   │   └── transform_transactions.py
│   │   └── gold/                    # Silver → Gold (aggregate)
│   │       └── aggregate_metrics.py
│   ├── common/
│   │   ├── spark_session.py         # SparkSession factory
│   │   ├── schemas.py               # Data schemas
│   │   └── quality.py               # Data quality checks
│   └── iceberg/                     # Apache Iceberg configuration
│       ├── catalog_setup.py
│       ├── maintenance.py
│       └── time_travel_examples.py
│
├── orchestration/                   # Apache Airflow
│   ├── dags/
│   │   ├── dag_daily_etl.py         # Daily ETL pipeline
│   │   ├── dag_streaming_monitor.py # Streaming health checks
│   │   └── dag_ml_pipeline.py       # Weekly ML training
│   └── plugins/
│       ├── operators/
│       │   └── spark_iceberg_operator.py
│       └── callbacks/
│           └── alerting.py
│
├── ml/                              # Machine Learning
│   ├── feature_store/
│   │   └── feature_repo/
│   │       ├── feature_definitions.py
│   │       ├── feature_store.yaml
│   │       └── data_sources.py
│   ├── training/
│   │   ├── train_anomaly_detector.py
│   │   ├── evaluate_model.py
│   │   └── promote_model.py
│   ├── serving/
│   │   ├── inference_service.py     # FastAPI inference endpoint
│   │   ├── model_loader.py
│   │   ├── prediction_logger.py
│   │   ├── Dockerfile
│   │   └── load_test.py
│   ├── config/
│   │   └── training_config.yaml
│   └── notebooks/
│       └── exploration.ipynb
│
├── monitoring/                      # Observability Stack
│   ├── prometheus/
│   │   ├── prometheus.yml           # Prometheus configuration
│   │   └── alert_rules.yml
│   ├── grafana/
│   │   └── provisioning/
│   │       ├── datasources/
│   │       └── dashboards/
│   │           ├── pipeline_overview.json
│   │           └── cost_tracking.json
│   └── alertas/
│       └── notification_channels.yaml
│
├── scripts/                         # Utilities and Automation
│   └── ops/
│       ├── reprocess_partition.py
│       ├── health_check.py
│       ├── data_quality_report.py
│       ├── cleanup_orphan_files.py
│       ├── rotate_secrets.py
│       └── generate_runbook.py
│
├── tests/                           # Test Suite
│   ├── ingestion/                   # Streaming tests
│   ├── etl/                         # Spark job tests
│   ├── orchestration/               # DAG tests
│   └── ml/                          # Model tests
│
├── .github/workflows/               # CI/CD Pipelines
│   ├── terraform-ci.yml
│   ├── terraform-cd.yml
│   ├── python-ci.yml
│   └── spark-tests.yml
│
├── docs/                            # Documentation
│   ├── architecture.md
│   ├── runbook.md
│   ├── onboarding.md
│   ├── data_catalog.md
│   └── monitoring_guide.md
│
├── docker-compose.yml               # Local development environment
├── docker-compose.monitoring.yml    # Monitoring stack
├── Makefile                         # Development shortcuts
├── pyproject.toml                   # Python project configuration
├── .gitignore
└── README.md                        # This file
```

## 💻 Technology Stack

### Data Processing
- **Apache Spark** (3.5.x) - Distributed data processing
- **Apache Iceberg** - Open table format with ACID, time travel
- **PySpark** - Python API for Spark
- **Pandas** - Data manipulation and analysis

### Streaming
- **Redpanda** - Kafka-compatible streaming platform
- **Debezium** - CDC (Change Data Capture)
- **Redpanda Connect** - Stream processing and sinking

### Orchestration & Workflow
- **Apache Airflow** - Workflow orchestration and scheduling
- **Python Click** - CLI utilities

### ML & Feature Engineering
- **Feast** - Feature Store
- **MLflow** - ML tracking and model registry
- **scikit-learn** - ML algorithms
- **XGBoost** - Gradient boosting

### Infrastructure & Cloud
- **Terraform** - Infrastructure as Code
- **AWS** (S3, IAM, Glue, EMR, Redshift)
- **Docker & Docker Compose** - Containerization

### API & Serving
- **FastAPI** - Modern async web framework
- **Uvicorn** - ASGI server

### Monitoring & Observability
- **Prometheus** - Metrics collection
- **Grafana** - Visualization and dashboards
- **AlertManager** - Alert routing and management
- **structlog** - Structured logging

### Testing & Code Quality
- **pytest** - Testing framework
- **pytest-cov** - Coverage reporting
- **ruff** - Fast Python linter
- **black** - Code formatter
- **mypy** - Static type checking
- **isort** - Import sorting

## 📦 Prerequisites

### Required
- Python 3.11+
- Docker Desktop (or Docker Engine + Docker Compose)
- Git
- AWS CLI (configured with credentials for AWS deployment)
- Terraform >= 1.7.0

### Optional (for full local development)
- Java 11+ (for Spark)
- PostgreSQL client tools
- Kubernetes CLI (for future k8s deployment)

## 🚀 Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone https://github.com/your-org/fx-data-platform.git
cd fx-data-platform

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
make setup
```

### 2. Start Docker Services

```bash
# Start all supporting services (PostgreSQL, Redpanda, MinIO, Airflow, MLflow)
make docker-up

# Wait for services to be ready (check with health checks)
make health-check

# Populate database with test data
make seed
```

### 3. Verify Setup

```bash
# Run all tests
make test

# Check code formatting and linting
make lint

# Run health checks on all services
make health-check
```

### 4. Access Services

Once everything is running:

- **Redpanda Console**: http://localhost:8080
- **MinIO**: http://localhost:9001 (user: `minioadmin`, password: `minioadmin`)
- **Airflow**: http://localhost:8090
- **MLflow**: http://localhost:5000
- **PostgreSQL**: `localhost:5432` (user: `postgres`, password: `postgres`)

## 💡 Development Workflow

### Creating a New ETL Job

1. Create a new file in `etl/jobs/{layer}/` (e.g., `my_new_job.py`)
2. Use the template from existing jobs
3. Write unit tests in `tests/etl/`
4. Create DAG task in `orchestration/dags/`
5. Test locally: `spark-submit etl/jobs/...my_new_job.py --env dev`

### Adding a New Feature to Feature Store

1. Define feature view in `ml/feature_store/feature_repo/feature_definitions.py`
2. Configure data source in `data_sources.py`
3. Test with: `feast apply` and `feast feature-views list`
4. Create materialization task in Airflow DAG
5. Write feature validation tests

### Training and Deploying a New Model

1. Prepare training script in `ml/training/`
2. Use MLflow for tracking: `mlflow.log_param()`, `mlflow.log_metric()`
3. Evaluate with `ml/training/evaluate_model.py`
4. Promote to staging/production with `ml/training/promote_model.py`
5. Deploy to serving API (automatically pulls from MLflow Model Registry)

## ✅ Testing

```bash
# Run all tests
make test

# Run only unit tests
make test-unit

# Run only integration tests (requires Docker)
make test-integration

# Run specific test file
pytest tests/etl/test_spark_jobs.py -v

# Run with coverage report
pytest tests/ --cov=etl,ml,ingestion --cov-report=html
```

## 🌐 Deployment

### Local Development
```bash
make docker-up
make docker-down
```

### AWS Deployment
```bash
cd infra/terraform/environments/dev
terraform init
terraform plan
terraform apply
```

### CI/CD Pipeline
Push to `main` branch triggers automated:
- Code formatting checks
- Unit and integration tests
- Terraform validation
- Automatic deployment (via GitHub Actions)

## 📚 Documentation

See the [docs/](docs/) directory for detailed documentation:

- [Architecture decisions and trade-offs](docs/architecture.md)
- [Operational runbook for troubleshooting](docs/runbook.md)
- [Onboarding guide for new team members](docs/onboarding.md)
- [Data catalog with schema and SLAs](docs/data_catalog.md)
- [Monitoring dashboard guide](docs/monitoring_guide.md)

## 🛠️ Useful Commands

```bash
# Code quality
make format        # Format all Python code
make lint          # Run linters and type checks
make clean         # Remove build artifacts

# Docker
make docker-up     # Start all services
make docker-down   # Stop all services
make docker-logs   # View Docker logs
make seed          # Populate test data
make reset         # Reset environment (destroy volumes)

# Testing & Monitoring
make test          # Run full test suite
make health-check  # Check all service health
make monitoring-up # Start monitoring stack (Prometheus, Grafana)
```

## 📊 Monitoring and Alerting

Access Grafana dashboards at http://localhost:3000:

- **Pipeline Overview**: ETL status, data freshness, quality metrics
- **ML Model Performance**: Prediction latency, data drift, F1 scores
- **Cost Tracking**: S3 storage, compute costs, anomaly detection

Alerts configured for:
- Consumer lag threshold exceeded
- Data freshness SLA breached
- Model performance degradation
- Infrastructure failures

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes and commit: `git commit -am 'Add your feature'`
3. Ensure tests pass: `make test`
4. Push and create Pull Request
5. Code review required before merge

## 📝 License

MIT License - see LICENSE file for details

## 👥 Team & Support

- **Data Engineering Lead**: data-team@example.com
- **On-call Runbook**: See [docs/runbook.md](docs/runbook.md)
- **Slack Channel**: #data-platform

---

**Latest Update**: March 2026  
**Version**: 0.1.0-alpha  
**Status**: 🔧 In Development
