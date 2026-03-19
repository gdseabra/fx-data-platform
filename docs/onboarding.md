# FX Data Platform — Onboarding Guide

Guia completo para novos membros da equipe: do zero ate rodar o pipeline completo.

## Pre-requisitos

| Software | Versao minima | Verificacao | Instalacao |
|----------|---------------|-------------|------------|
| Docker Desktop | 24.0+ | `docker --version` | https://docs.docker.com/get-docker/ |
| Docker Compose | v2.20+ | `docker compose version` | Incluido no Docker Desktop |
| Python | 3.10+ | `python3 --version` | pyenv ou conda |
| Conda (Miniconda) | - | `conda --version` | https://docs.conda.io/en/latest/miniconda.html |
| Git | 2.30+ | `git --version` | https://git-scm.com/ |
| AWS CLI | 2.x (opcional) | `aws --version` | https://aws.amazon.com/cli/ |
| Terraform | 1.7+ (opcional) | `terraform --version` | https://developer.hashicorp.com/terraform/install |

**Requisitos de sistema:** 8GB RAM minimo (16GB recomendado), 20GB disco livre para Docker.

---

## Setup do ambiente local (passo a passo)

### 1. Clonar o repositorio

```bash
git clone https://github.com/your-org/fx-data-platform.git
cd fx-data-platform
```

### 2. Criar ambiente Conda e instalar dependencias

```bash
# Criar env conda (usado pelo Spark via Makefile)
conda create -n nomad python=3.11 -y
conda activate nomad

# Instalar dependencias do projeto
make setup
# Equivale a: pip install -e ".[dev]"

# Verificar instalacao
python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"
python -c "import mlflow; print(f'MLflow {mlflow.__version__}')"
python -c "import feast; print(f'Feast {feast.__version__}')"
```

### 3. Subir servicos Docker

```bash
# Inicia PostgreSQL, Redpanda, MinIO, Airflow, MLflow, Redis, Inference Service
make docker-up

# Aguardar inicializacao (~30s para todos ficarem healthy)
# Verificar status
docker compose ps

# Health check completo
make health-check
```

### 4. Popular dados de teste

```bash
# Cria tabelas no PostgreSQL e insere dados de exemplo
make seed

# Verificar
docker compose exec postgres psql -U postgres -d fx_transactions -c "SELECT COUNT(*) FROM transactions;"
```

### 5. Rodar o pipeline ETL completo

```bash
# Criar catalogo e tabelas Iceberg (primeira vez)
make etl-setup

# Gerar dados de streaming (60 segundos, 100 msgs/s)
python -m ingestion.producer.streaming_producer --rate 100 --duration 60 --anomalies

# Rodar ETL Bronze → Silver → Gold
make etl-all

# Verificar resultados
# Os jobs Spark imprimem metricas ao final (rows_read, rows_written)
```

### 6. Verificar que tudo funciona

```bash
# Rodar testes unitarios
make test-unit

# Rodar linter
make lint
```

---

## Acessar servicos

| Servico | URL | Credenciais | Para que serve |
|---------|-----|-------------|----------------|
| PostgreSQL | `localhost:5434` | postgres / postgres | Banco transacional fonte |
| Redpanda Console | http://localhost:8080 | - | Visualizar topicos e mensagens |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin | Explorar buckets S3 |
| Airflow | http://localhost:8090 | admin / admin | Gerenciar e monitorar DAGs |
| MLflow | http://localhost:5000 | - | Tracking de experimentos e modelo registry |
| Inference API | http://localhost:8000/docs | - | Swagger UI do servico de inferencia |
| Prometheus | http://localhost:9090 | - | Metricas (apos `make monitoring-up`) |
| Grafana | http://localhost:3000 | admin / admin | Dashboards (apos `make monitoring-up`) |
| AlertManager | http://localhost:9093 | - | Alertas (apos `make monitoring-up`) |

---

## Como rodar o pipeline completo localmente

```mermaid
graph LR
    A[make docker-up] --> B[make seed]
    B --> C[Gerar streaming data]
    C --> D[make etl-setup]
    D --> E[make etl-bronze]
    E --> F[make etl-silver]
    F --> G[make etl-gold]
    G --> H[Verificar no MinIO/Spark]
```

```bash
# Pipeline completo de ponta a ponta
make docker-up
sleep 30
make seed

# Gerar dados simulados
python -m ingestion.producer.streaming_producer --rate 200 --duration 120 --anomalies &

# Aguardar dados serem gravados no MinIO pelo Redpanda Connect
sleep 30

# ETL completo
make etl-all
# Output: "Full ETL pipeline complete for YYYY-MM-DD"

# Subir monitoring para visualizar
make monitoring-up
# Acessar Grafana em http://localhost:3000
```

---

## Como criar um novo job ETL

### 1. Criar o arquivo do job

```bash
# Exemplo: novo job Silver para enriquecer com dados externos
touch etl/jobs/silver/enrich_with_external_rates.py
```

### 2. Seguir o padrao existente

```python
"""Silver enrichment job — joins external exchange rates."""

import argparse
from etl.common.spark_session import create_spark_session
from etl.common.schemas import TRANSACTION_SCHEMA
from etl.common.quality import QualityRunner

def main(env: str, date: str):
    spark = create_spark_session(env=env, app_name="silver-enrich-rates")

    # 1. Read from source
    df = spark.table("silver.transactions").filter(f"event_date = '{date}'")

    # 2. Transform
    # ... your logic here ...

    # 3. Quality checks
    runner = QualityRunner()
    runner.check_not_null(df, "exchange_rate")
    report = runner.run()
    print(report)

    # 4. Write to Iceberg
    df.writeTo("silver.enriched_transactions").overwritePartitions()

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.env, args.date)
```

### 3. Adicionar teste

```bash
# Criar teste em tests/etl/
touch tests/etl/test_enrich_with_external_rates.py
# Usar fixtures de conftest.py (spark_session, sample_data)
make test-unit
```

### 4. Adicionar ao Airflow DAG

Editar `orchestration/dags/dag_daily_etl.py` e adicionar uma task usando `SparkIcebergOperator`.

### 5. Testar localmente

```bash
SPARK_CONF_DIR=conf/ spark-submit etl/jobs/silver/enrich_with_external_rates.py --date 2024-03-15 --env dev
```

---

## Como adicionar uma nova feature ao Feast

### 1. Definir a feature

Editar `ml/feature_store/feature_repo/feature_definitions.py`:

```python
my_new_features = FeatureView(
    name="my_new_features",
    entities=[user_entity],
    schema=[
        Field(name="new_metric_30d", dtype=Float64),
    ],
    source=my_data_source,
    ttl=timedelta(days=1),
)
```

### 2. Definir o data source

Editar `ml/feature_store/feature_repo/data_sources.py`:

```python
my_data_source = FileSource(
    path="data/feature_store/sources/my_features.parquet",
    timestamp_field="event_timestamp",
)
```

### 3. Aplicar mudancas

```bash
cd ml/feature_store/feature_repo
feast apply  # Registra features no registry
```

### 4. Materializar para online store

```bash
python -m ml.feature_store.scripts.materialize_features
```

### 5. Testar

```python
from feast import FeatureStore
store = FeatureStore(repo_path="ml/feature_store/feature_repo")
features = store.get_online_features(
    features=["my_new_features:new_metric_30d"],
    entity_rows=[{"user_id": "test-user-001"}],
).to_dict()
print(features)
```

---

## Como retreinar e deployar o modelo

```bash
# 1. Treinar modelo
python -m ml.training.train_anomaly_detector --config ml/config/training_config.yaml --env dev

# 2. Abrir MLflow para ver resultados
# http://localhost:5000 → Experiment "fx-anomaly-detection"

# 3. Comparar com modelo atual
python -m ml.training.evaluate_model --env dev

# 4. Promover para producao (se metricas aprovadas)
python -m ml.training.promote_model --env dev
# Validacoes automaticas: F1 > 0.85, p95 latency < 50ms, precision > 0.80

# 5. Inference service faz hot reload automaticamente (verifica a cada 5min)
# Verificar:
curl http://localhost:8000/health | python -m json.tool
```

---

## Convencoes de codigo

### Formatacao
- **Black** com line length 100
- **isort** para ordenar imports
- Rodar antes de commit: `make format`

### Linting
- **ruff** para regras Python (E, F, W, I, N, D, UP, B, A, C4, PIE, T20, PT)
- **mypy** para type checking
- Rodar: `make lint`

### Testes
- **pytest** com markers: `@pytest.mark.unit`, `@pytest.mark.integration`
- Coverage minimo: 80%
- Fixtures compartilhadas em `tests/conftest.py`
- Rodar: `make test`

### Commits
- Mensagens em ingles, imperativo: "Add feature X", "Fix bug in Y"
- PRs revisadas por pelo menos 1 pessoa
- CI/CD roda automaticamente em PRs

### Estrutura de arquivos
- Jobs ETL em `etl/jobs/{layer}/`
- Schemas em `etl/common/schemas.py`
- DAGs em `orchestration/dags/`
- Features em `ml/feature_store/feature_repo/`
- Testes espelham a estrutura do codigo: `tests/{modulo}/test_{arquivo}.py`

---

## Comandos rapidos

```bash
# Desenvolvimento
make setup              # Instalar dependencias
make format             # Formatar codigo
make lint               # Rodar linters
make test               # Rodar testes com coverage

# Docker
make docker-up          # Subir servicos
make docker-down        # Parar servicos
make logs               # Ver logs
make seed               # Popular banco
make reset              # Destruir volumes e recriar

# ETL
make etl-setup          # Criar tabelas Iceberg
make etl-all            # Pipeline completo (Bronze→Silver→Gold)

# Monitoring
make monitoring-up      # Subir Prometheus/Grafana/AlertManager
make health-check       # Verificar saude de todos os servicos

# Limpeza
make clean              # Remover artefatos de build
```

---

## Troubleshooting comum

| Problema | Solucao |
|----------|---------|
| "Port already in use" | `lsof -i :<port>` e `kill -9 <PID>` |
| Docker sem espaco | `docker system prune -af` |
| Spark "ClassNotFound" Iceberg | Verificar `conf/spark-defaults.conf` e que JARs foram baixados (~200MB, cached em ~/.ivy2) |
| Airflow "connection refused" | Aguardar init container: `docker compose logs airflow-init` |
| MinIO buckets faltando | `docker compose restart minio-setup` |
| Testes falhando com SparkSession | Verificar que conda env `nomad` esta ativo |

---

## Proximos passos apos setup

1. Ler a [Arquitetura](./architecture.md) para entender o design
2. Explorar o Airflow em http://localhost:8090 — ver as 3 DAGs
3. Rodar o pipeline ETL e verificar dados no MinIO
4. Fazer uma alteracao pequena, rodar testes, criar PR
5. Ler o [Runbook](./runbook.md) para entender procedimentos operacionais

---

**Last Updated:** March 2026
