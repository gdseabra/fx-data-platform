# Prompts Passo-a-Passo — Projeto FX Data Platform

## Como usar este documento

Cada fase contém **prompts numerados** para colar diretamente no Claude Code.
Siga a ordem dentro de cada fase. Ao final de cada prompt, valide o resultado antes de avançar.

> **Dica:** Antes de começar, crie o repositório e clone localmente:
> ```bash
> git init fx-data-platform && cd fx-data-platform
> ```

---

## FASE 1 — Fundação da Infraestrutura (Terraform + AWS + CI/CD)

### Prompt 1.1 — Estrutura do repositório

```
Crie a estrutura de diretórios de um projeto de engenharia de dados chamado "fx-data-platform". É uma plataforma de dados para processamento de transações de câmbio em tempo real com detecção de anomalias via ML. O projeto usa:
- Terraform para IaC (AWS)
- Python + PySpark para ETL
- Airflow para orquestração
- MLflow + Feast para MLOps
- GitHub Actions para CI/CD

Crie a estrutura de pastas seguindo boas práticas de monorepo:

fx-data-platform/
├── infra/terraform/          (módulos Terraform: s3, iam, glue, emr, redpanda, monitoring)
├── ingestion/                (scripts de ingestão e conectores)
├── etl/                      (jobs PySpark: bronze, silver, gold)
├── orchestration/            (DAGs Airflow)
├── ml/                       (pipelines de ML: training, serving, feature store)
├── monitoring/               (dashboards, alertas, observabilidade)
├── scripts/                  (utilitários e automações)
├── tests/                    (testes unitários e de integração)
├── .github/workflows/        (CI/CD)
├── docs/                     (documentação e diagramas)
├── docker-compose.yml        (ambiente local de desenvolvimento)
├── Makefile                  (atalhos de comandos)
├── pyproject.toml            (dependências Python)
└── README.md                 (visão geral do projeto com diagrama de arquitetura)

Crie todos os arquivos com conteúdo inicial (não deixe vazios). Cada arquivo deve ter comentários explicando seu propósito. O README.md deve conter um diagrama ASCII da arquitetura completa do projeto. Inclua um .gitignore completo para Python, Terraform, Node e arquivos de IDE.
```

### Prompt 1.2 — Módulos Terraform (S3 + Glue + IAM)

```
No diretório infra/terraform/, crie os seguintes módulos Terraform para AWS:

1. **modules/s3/** — Crie buckets S3 para o Data Lakehouse com as seguintes camadas:
   - fx-datalake-raw (dados brutos da ingestão)
   - fx-datalake-bronze (dados limpos com schema)
   - fx-datalake-silver (dados transformados e enriquecidos)
   - fx-datalake-gold (dados prontos para consumo)
   - fx-datalake-ml (artefatos de ML: modelos, features, métricas)
   Aplique: versionamento, encryption (SSE-S3), lifecycle rules (mover para Glacier após 90 dias no raw), tags de custo e block public access.

2. **modules/iam/** — Roles e policies para:
   - Role para Glue Jobs (acesso aos buckets + CloudWatch Logs)
   - Role para EMR Serverless (acesso aos buckets + Glue Catalog)
   - Role para Lambda (se necessário para triggers)
   - Policy de least privilege para cada role

3. **modules/glue/** — Glue Catalog:
   - Database "fx_bronze", "fx_silver", "fx_gold"
   - Crawler configurado para catalogar os buckets bronze e silver
   - Glue Job shell para ETL com PySpark apontando para os scripts no S3

4. **environments/** — Crie um ambiente "dev" com terraform.tfvars que use variáveis para nomear recursos com prefixo "dev-".

5. **main.tf + variables.tf + outputs.tf** na raiz de infra/terraform/ orquestrando os módulos.

Use variáveis para região (default: us-east-1), environment, project_name. Todos os recursos devem ter tags padrão: Project, Environment, ManagedBy=terraform. Escreva código Terraform limpo, modular, com outputs úteis (ARNs, nomes de bucket).
```

### Prompt 1.3 — CI/CD com GitHub Actions

```
Crie pipelines de CI/CD no diretório .github/workflows/:

1. **terraform-ci.yml** — Acionado em PRs que alterem infra/terraform/**:
   - Step 1: Checkout do código
   - Step 2: Setup Terraform (versão 1.7+)
   - Step 3: terraform fmt -check (falha se não estiver formatado)
   - Step 4: terraform init (com backend S3 configurável)
   - Step 5: terraform validate
   - Step 6: terraform plan -out=plan.tfplan
   - Step 7: Posta o output do plan como comentário na PR usando github-script
   - Usa OIDC para autenticação com AWS (sem secrets de access key)

2. **terraform-cd.yml** — Acionado em merge na branch main (paths: infra/terraform/**):
   - terraform init + terraform apply -auto-approve
   - Notificação de sucesso/falha (pode ser via Slack webhook ou comment no commit)

3. **python-ci.yml** — Acionado em PRs que alterem etl/**, ml/**, ingestion/**:
   - Setup Python 3.11
   - Install dependencies (pip install -e ".[dev]")
   - Lint com ruff
   - Type check com mypy
   - Testes com pytest (com coverage report)
   - Falha se coverage < 80%

4. **spark-tests.yml** — Acionado em PRs que alterem etl/**:
   - Roda testes PySpark usando pyspark local (sem cluster)
   - Valida schemas dos DataFrames de saída

Todos os workflows devem ter: concurrency group (evitar runs paralelos), cache de dependências, e badges no README.
```

### Prompt 1.4 — Docker Compose para ambiente local

```
Crie um docker-compose.yml completo para desenvolvimento local do projeto. O ambiente deve subir:

1. **PostgreSQL 15** — Simula o banco transacional de uma fintech de câmbio
   - Porta: 5432
   - Database: fx_transactions
   - Inclua um script de inicialização (init.sql) que crie as tabelas:
     - users (user_id UUID PK, name, cpf, email, state, tier, registration_date, avg_monthly_volume)
     - transactions (transaction_id UUID PK, user_id FK, timestamp, transaction_type, currency, amount_brl, amount_foreign, exchange_rate, spread_pct, fee_brl, status, channel, device)
   - Popule com 100 registros de exemplo usando o simulador Python que já temos

2. **Redpanda** (substituto leve do Kafka para dev local)
   - Porta: 9092 (Kafka API), 8081 (Schema Registry), 8082 (REST Proxy)
   - Crie tópicos automáticos: fx.transactions, fx.exchange-rates, fx.users-cdc
   - Console do Redpanda na porta 8080

3. **Redpanda Connect** — Configurado com pipeline:
   - Input: CDC do PostgreSQL (polling ou via Debezium)
   - Output: tópico fx.transactions no Redpanda

4. **MinIO** — Substituto local do S3
   - Porta: 9000 (API), 9001 (Console)
   - Crie os mesmos buckets do Terraform (raw, bronze, silver, gold, ml)

5. **Apache Airflow** (standalone/local)
   - Porta: 8090
   - DAGs do diretório orchestration/
   - Conexões pré-configuradas para MinIO e Redpanda

6. **MLflow Tracking Server**
   - Porta: 5000
   - Backend store: SQLite (para dev)
   - Artifact store: MinIO (bucket fx-datalake-ml)

Crie um Makefile com comandos:
- make up (sobe tudo)
- make down (para tudo)
- make seed (popula o PostgreSQL com dados simulados)
- make logs (mostra logs de todos os serviços)
- make reset (destrói volumes e recria tudo)

Adicione healthchecks em todos os serviços e dependências corretas (depends_on com condition: service_healthy).
```

---

## FASE 2 — Ingestão e Streaming (Redpanda + Debezium + CDC)

### Prompt 2.1 — Configuração do CDC com Debezium

```
No diretório ingestion/, crie a configuração completa de Change Data Capture:

1. **debezium/connector-config.json** — Configuração do conector Debezium para PostgreSQL:
   - Captura CDC das tabelas: users, transactions
   - Formato: JSON com schema
   - Slot name: fx_cdc_slot
   - Publication: fx_publication
   - Snapshot mode: initial (captura estado inicial + mudanças)
   - Tópicos de destino: fx.public.users, fx.public.transactions
   - Transforms: route por tabela, unwrap (flatten do envelope Debezium)
   - Heartbeat interval: 10 segundos

2. **debezium/register-connector.sh** — Script para registrar o conector via Kafka Connect REST API.

3. **redpanda-connect/pipeline.yaml** — Pipeline do Redpanda Connect:
   - Input: consome do tópico fx.public.transactions no Redpanda
   - Processamento: parse JSON, adiciona metadata (ingestion_timestamp, source_system, batch_id)
   - Output: grava no MinIO (ou S3) no bucket raw, particionado por event_date=YYYY-MM-DD, formato Parquet
   - Error handling: dead letter queue para mensagens malformadas
   - Inclua métricas Prometheus

4. **schemas/** — JSON Schemas para validação das mensagens:
   - transaction_event.json
   - user_event.json

Documente no README da pasta como subir o CDC completo do zero e como verificar se está funcionando (comandos de verificação para cada componente).
```

### Prompt 2.2 — Produtor Python para simulação de streaming

```
No diretório ingestion/, crie um produtor Python robusto para simular streaming de dados:

1. **producer/streaming_producer.py** — Produtor Kafka/Redpanda que:
   - Usa a biblioteca confluent-kafka (compatível com Redpanda)
   - Publica transações simuladas no tópico fx.transactions
   - Controle de throughput configurável (ex: 100 msgs/seg, 1000 msgs/seg)
   - Modo burst (simula picos de Black Friday, câmbio volátil)
   - Serialização com Avro ou JSON Schema (registrado no Schema Registry)
   - Callbacks de delivery confirmation com logging
   - Graceful shutdown com signal handling (SIGINT, SIGTERM)
   - Métricas: mensagens enviadas, latência, erros, throughput

2. **producer/exchange_rate_producer.py** — Produtor dedicado para taxas de câmbio:
   - Publica taxas atualizadas a cada 5 segundos no tópico fx.exchange-rates
   - Usa o simulador de random walk do nosso fx_data_simulator.py
   - Inclui flag de anomalia quando a variação excede 2 desvios padrão

3. **producer/config.py** — Configurações centralizadas:
   - Bootstrap servers, Schema Registry URL
   - Configurações de producer (acks, retries, batch_size, linger_ms)
   - Feature flags (enable_anomalies, anomaly_rate)

4. **consumer/health_check_consumer.py** — Consumer simples que:
   - Consome dos 3 tópicos e imprime estatísticas a cada 10 segundos
   - Mostra: lag, throughput, última mensagem, erros
   - Útil para validar que a ingestão está funcionando

Crie um CLI com argparse ou click para controlar os produtores:
  python -m ingestion.producer.streaming_producer --rate 500 --duration 60 --anomalies
```

### Prompt 2.3 — Testes de ingestão

```
No diretório tests/ingestion/, crie testes completos para a camada de ingestão:

1. **test_producer.py** — Testes unitários:
   - Testa serialização de mensagens (schema correto)
   - Testa geração de chaves de particionamento
   - Testa injeção de anomalias (rate configurável)
   - Testa tratamento de erros de conexão (mock do producer)
   - Testa configurações de throughput

2. **test_schemas.py** — Validação de schemas:
   - Valida que mensagens geradas conformam com os JSON Schemas
   - Testa backward compatibility de schemas (evolução)
   - Testa mensagens malformadas são rejeitadas

3. **test_integration_streaming.py** — Teste de integração (usa testcontainers ou docker):
   - Sobe Redpanda em container
   - Publica N mensagens, consome e valida
   - Verifica ordering dentro de partição
   - Verifica que dead letter queue captura mensagens inválidas

Use pytest com fixtures, markers (@pytest.mark.integration para testes pesados), e conftest.py com factories de dados de teste.
```

---

## FASE 3 — ETL/ELT e Modelagem de Dados (Spark + Iceberg + Airflow)

### Prompt 3.1 — Jobs PySpark (Bronze → Silver → Gold)

```
No diretório etl/, crie os jobs PySpark para as transformações do Data Lakehouse:

1. **jobs/bronze/ingest_transactions.py** — Raw → Bronze:
   - Lê Parquet da camada raw no S3 (particionado por event_date)
   - Schema enforcement com StructType explícito
   - Deduplicação por transaction_id (keep latest by ingestion_timestamp)
   - Adiciona colunas de auditoria: _bronze_loaded_at, _source_file, _batch_id
   - Grava como tabela Iceberg: fx_bronze.transactions
   - Modo: merge/upsert (não reprocessa o que já existe)
   - Logging estruturado com métricas (rows_read, rows_written, rows_deduplicated)

2. **jobs/silver/transform_transactions.py** — Bronze → Silver:
   - Lê da tabela Iceberg fx_bronze.transactions
   - Limpeza: remove registros com amount_brl <= 0, normaliza currency codes
   - Enriquecimento: join com tabela de users (nome, tier, state)
   - Tipagem forte: cast de timestamps, decimals para valores monetários
   - Colunas derivadas: amount_usd (converte tudo para USD base), day_of_week, hour_of_day, is_business_hours
   - Validações (Great Expectations inline): amount_brl > 0, currency in lista válida, user_id existe na tabela users
   - Grava como tabela Iceberg: fx_silver.transactions
   - Particiona por: event_date, currency

3. **jobs/gold/aggregate_metrics.py** — Silver → Gold:
   - Cria tabelas agregadas para consumo direto:
     a) gold_daily_volume: volume diário por moeda (sum, count, avg, median, p95)
     b) gold_user_summary: resumo por usuário (total transações, volume, moedas usadas, primeiro/último uso, tier)
     c) gold_hourly_rates: taxa de câmbio média por hora por moeda (para gráficos)
     d) gold_anomaly_summary: contagem de anomalias por tipo por dia
   - Todas como tabelas Iceberg no database fx_gold
   - Inclua window functions para calcular: variação dia-a-dia, média móvel 7 dias, ranking de usuários por volume

4. **common/spark_session.py** — Factory de SparkSession:
   - Configuração padrão para Iceberg (catalog, warehouse path)
   - Configuração para S3 (credenciais via IAM role ou MinIO local)
   - Helper para detectar ambiente (local vs EMR) e ajustar configs
   - Logging configurado

5. **common/schemas.py** — Schemas centralizados:
   - StructType para cada entidade (transaction, user, exchange_rate)
   - Funções de validação de schema

6. **common/quality.py** — Framework de quality checks:
   - Funções reutilizáveis: check_not_null, check_unique, check_range, check_referential_integrity
   - Cada check retorna um QualityReport com métricas
   - Integração com logging para enviar métricas ao CloudWatch

Cada job deve ser executável como:
  spark-submit etl/jobs/bronze/ingest_transactions.py --date 2024-01-15 --env dev
```

### Prompt 3.2 — Configuração do Apache Iceberg

```
Crie a configuração completa do Apache Iceberg para o projeto:

1. **etl/iceberg/catalog_setup.py** — Script PySpark que:
   - Cria o Iceberg catalog (tipo: Glue Catalog para AWS, ou Hive local)
   - Cria os namespaces: fx_bronze, fx_silver, fx_gold
   - Cria as tabelas com schema explícito e partitioning strategy:
     - bronze.transactions: particionado por days(event_date)
     - silver.transactions: particionado por days(event_date), bucket(16, currency)
     - gold.*: particionado conforme granularidade de cada tabela
   - Configura table properties: write.format.default=parquet, commit.retry.num-retries=3

2. **etl/iceberg/maintenance.py** — Script de manutenção:
   - Expire snapshots (manter últimos 7 dias)
   - Remove orphan files
   - Compactação de small files (rewrite_data_files)
   - Pode ser chamado pelo Airflow como task de manutenção diária

3. **etl/iceberg/time_travel_examples.py** — Exemplos de uso:
   - Query em snapshot específico (auditoria)
   - Rollback de tabela para versão anterior
   - Incremental read (mudanças desde último snapshot)

Documente no código como o Iceberg resolve problemas comuns: ACID transactions, schema evolution, time travel, e hidden partitioning.
```

### Prompt 3.3 — DAGs do Airflow

```
No diretório orchestration/dags/, crie as DAGs do Airflow:

1. **dag_daily_etl.py** — Pipeline ETL diário:
   - Schedule: diário às 06:00 UTC
   - Tasks (em ordem com dependências):
     a) sensor_check_raw_data: S3KeySensor verifica se partição do dia existe no raw
     b) bronze_ingest: SparkSubmitOperator roda ingest_transactions.py
     c) quality_check_bronze: PythonOperator roda quality checks na bronze
     d) silver_transform: SparkSubmitOperator roda transform_transactions.py
     e) quality_check_silver: PythonOperator roda quality checks na silver
     f) gold_aggregate: SparkSubmitOperator roda aggregate_metrics.py
     g) iceberg_maintenance: PythonOperator roda compactação e expire snapshots
     h) notify_success: notificação de conclusão (Slack/email)
   - Error handling: on_failure_callback envia alerta com contexto do erro
   - Retries: 2 com exponential backoff
   - SLA: 2 horas (alerta se ultrapassar)
   - Tags: ["etl", "daily", "production"]
   - Catchup: False
   - Templated dates: {{ ds }} para processar data correta

2. **dag_streaming_monitor.py** — Monitoramento do streaming:
   - Schedule: a cada 15 minutos
   - Tasks:
     a) check_redpanda_lag: verifica consumer lag dos tópicos
     b) check_connector_status: verifica se Debezium/Redpanda Connect estão healthy
     c) check_data_freshness: verifica se última mensagem no S3 raw tem < 30 min
     d) alert_if_stale: envia alerta se dados estiverem atrasados

3. **dag_ml_pipeline.py** — Pipeline de ML (será detalhado na Fase 4, crie um placeholder):
   - Schedule: semanal
   - Tasks: feature_engineering → training → validation → deployment
   - Placeholder com comentários explicando o que cada task fará

4. **plugins/operators/spark_iceberg_operator.py** — Operator customizado:
   - Wrapper do SparkSubmitOperator com configs padrão do projeto
   - Adiciona automaticamente configs do Iceberg e S3
   - Logging melhorado com métricas

5. **plugins/callbacks/alerting.py** — Callbacks reutilizáveis:
   - on_failure_callback: envia alerta com task, dag, execution_date, log link
   - on_sla_miss_callback: alerta de SLA com tempo real vs esperado
   - on_success_callback: notificação de conclusão com métricas resumidas

Inclua um conftest no diretório tests/ para testar as DAGs (DagBag validation, no import errors, correct schedule, etc.).
```

---

## FASE 4 — MLOps e Servimento de Modelos (MLflow + Feast + Inferência)

### Prompt 4.1 — Feature Store com Feast

```
No diretório ml/feature_store/, configure o Feast:

1. **feature_repo/feature_definitions.py** — Defina as features:
   - Entity: user (join key: user_id)
   - Entity: currency_pair (join key: currency)
   
   Feature Views:
   a) user_transaction_features (source: gold_user_summary):
      - total_transactions_30d, total_volume_30d, avg_ticket_30d
      - preferred_currency, transaction_count_by_type
      - days_since_registration, tier
      - TTL: 1 dia
   
   b) currency_features (source: gold_hourly_rates):
      - current_rate, rate_change_1h, rate_change_24h
      - volatility_7d (std dev dos últimos 7 dias)
      - avg_spread, volume_24h
      - TTL: 1 hora
   
   c) transaction_pattern_features (source: silver.transactions, on-demand):
      - transactions_last_1h (por user)
      - avg_amount_last_24h
      - distinct_currencies_last_7d
      - night_transaction_ratio (% de transações entre 0h-6h)
   
   On-demand feature view:
   d) realtime_anomaly_features:
      - Input: transaction amount, timestamp, user features
      - Calcula: z_score do amount vs média do usuário
      - Calcula: is_unusual_hour (boolean)
      - Calcula: velocity (transações por hora recente)

2. **feature_repo/feature_store.yaml** — Configuração:
   - Provider: aws (ou local para dev)
   - Registry: S3 (bucket fx-datalake-ml/feast-registry)
   - Online store: DynamoDB (ou SQLite local)
   - Offline store: S3 com Iceberg/Parquet

3. **feature_repo/data_sources.py** — Data sources apontando para tabelas Iceberg no Glue Catalog.

4. **scripts/materialize_features.py** — Script para materializar features:
   - Materializa do offline store para online store
   - Configurável por feature view
   - Pode rodar como task do Airflow

5. **scripts/validate_features.py** — Validação de features:
   - Verifica freshness do online store
   - Compara distribuição online vs offline (detect drift)
   - Gera relatório de qualidade

Inclua exemplos de como consumir features:
  - Offline: para training (feast.get_historical_features)
  - Online: para inferência em tempo real (feast.get_online_features)
```

### Prompt 4.2 — Pipeline de treinamento com MLflow

```
No diretório ml/training/, crie o pipeline completo de treinamento:

1. **train_anomaly_detector.py** — Pipeline principal:
   - Busca features históricas do Feast (offline store) para os últimos 90 dias
   - Prepara dataset: features numéricas + encoding de categóricas
   - Split temporal (não random!): train=70%, validation=15%, test=15% (por data)
   - Treina Isolation Forest (sklearn) para detecção de anomalias em transações de câmbio
   - Também treina um modelo alternativo: LOF (Local Outlier Factor) para comparação
   
   Integração com MLflow:
   - mlflow.set_experiment("fx-anomaly-detection")
   - Log de hiperparâmetros: contamination, n_estimators, max_features, random_state
   - Log de métricas: precision, recall, f1 (usando as anomalias injetadas como ground truth), AUC-ROC, latência de inferência (p50, p95, p99)
   - Log de artefatos: modelo serializado, feature importance plot, confusion matrix plot, distribuição de scores
   - Log do dataset: tamanho, período, features usadas
   - Tags: model_type, data_version, environment

2. **evaluate_model.py** — Avaliação comparativa:
   - Carrega os N últimos runs do experimento
   - Compara métricas lado a lado
   - Gera relatório HTML com gráficos (precision-recall curve, score distribution)
   - Recomenda o melhor modelo baseado em F1 + latência

3. **promote_model.py** — Promoção no Model Registry:
   - Registra o modelo vencedor no MLflow Model Registry
   - Transiciona: None → Staging → Production
   - Validações antes de promover para Production:
     a) F1 > threshold (ex: 0.85)
     b) Latência p95 < 50ms
     c) Modelo não é pior que o atual em produção
   - Log de quem promoveu, quando e por quê

4. **config/training_config.yaml** — Configurações do pipeline:
   - Hiperparâmetros e seus ranges para tuning
   - Thresholds de promoção
   - Feature list
   - Período de dados para treino

5. **notebooks/exploration.ipynb** — Jupyter notebook de exploração:
   - EDA dos dados de transações de câmbio
   - Análise de anomalias injetadas vs detectadas
   - Visualizações das features mais importantes
   - Serve como documentação viva do modelo

Tudo deve ser executável como:
  python -m ml.training.train_anomaly_detector --config ml/config/training_config.yaml --env dev
```

### Prompt 4.3 — Servimento e inferência em tempo real

```
No diretório ml/serving/, crie o sistema de inferência:

1. **inference_service.py** — Serviço de inferência com FastAPI:
   - Endpoint POST /predict:
     - Recebe: transaction_id, user_id, amount_brl, currency, timestamp
     - Busca features online do Feast para o user_id
     - Calcula on-demand features (z_score, velocity)
     - Carrega modelo do MLflow Model Registry (versão "Production")
     - Retorna: anomaly_score (0-1), is_anomaly (bool), risk_level (low/medium/high), explanation (top features que contribuíram)
     - Latência alvo: < 100ms p95
   
   - Endpoint GET /health:
     - Verifica: modelo carregado, Feast online store acessível, latência ok
   
   - Endpoint GET /metrics:
     - Prometheus metrics: request_count, latency_histogram, prediction_distribution, error_rate

2. **model_loader.py** — Cache e reload de modelos:
   - Carrega modelo do MLflow Registry na inicialização
   - Cache em memória
   - Background task que verifica nova versão a cada 5 minutos
   - Hot reload sem downtime quando nova versão disponível
   - Fallback para versão anterior se nova versão falhar

3. **prediction_logger.py** — Log de predições:
   - Loga cada predição: input features, output score, latência, modelo versão
   - Grava em tópico Redpanda (fx.predictions) para análise posterior
   - Dados usados para calcular drift e monitoramento

4. **Dockerfile** — Container da API:
   - Base: python:3.11-slim
   - Instala dependências mínimas
   - Healthcheck configurado
   - Non-root user
   - Multi-stage build para imagem menor

5. **scripts/load_test.py** — Teste de carga com locust:
   - Simula 100 requests/segundo
   - Mede latência p50, p95, p99
   - Verifica error rate < 0.1%
   - Gera relatório HTML

Adicione o serviço ao docker-compose.yml:
  - Porta: 8000
  - Depende de: MinIO, Redpanda, MLflow
  - Health check via /health
```

---

## FASE 5 — Observabilidade e Operação (Monitoramento + Alertas)

### Prompt 5.1 — Stack de observabilidade

```
No diretório monitoring/, crie a stack completa de observabilidade:

1. **docker-compose.monitoring.yml** — Serviços de monitoramento:
   - Prometheus (porta 9090): scrape de métricas
   - Grafana (porta 3000): dashboards
   - AlertManager (porta 9093): gerenciamento de alertas
   
   Configurações:
   - prometheus/prometheus.yml: scrape configs para todos os serviços (Redpanda, FastAPI, Airflow, MinIO)
   - prometheus/alert_rules.yml: regras de alerta
   - grafana/provisioning/: datasources e dashboards provisionados automaticamente

2. **grafana/dashboards/pipeline_overview.json** — Dashboard principal:
   Painéis:
   a) Pipeline Health:
      - Status de cada etapa (ingestão → bronze → silver → gold)
      - Última execução bem-sucedida de cada job
      - SLA compliance (% de execuções dentro do SLA)
   
   b) Streaming Metrics:
      - Mensagens/segundo por tópico no Redpanda
      - Consumer lag por consumer group
      - Tamanho das partições
      - Taxa de erro do Debezium/Redpanda Connect
   
   c) Data Quality:
      - Freshness de cada camada (minutos desde última atualização)
      - Contagem de registros por camada (com tendência)
      - Percentual de registros que passaram nos quality checks
      - Anomalias detectadas vs injetadas (para validação)
   
   d) ML Model Performance:
      - Prediction latency (p50, p95, p99) em tempo real
      - Score distribution (histograma atualizado a cada minuto)
      - Data drift detection (PSI score por feature)
      - Concept drift (F1 do modelo ao longo do tempo, usando labels retroativos)
      - Model version em produção e quando foi deployado

3. **grafana/dashboards/cost_tracking.json** — Dashboard de custos:
   - Estimativa de custo S3 por camada
   - Custo estimado de EMR/Glue jobs
   - Custo de Redpanda por throughput

4. **alertas/alert_rules.yml** — Regras de alerta do Prometheus:
   - CRITICAL: consumer lag > 10.000 por mais de 5 minutos
   - CRITICAL: nenhum dado novo na camada raw há mais de 1 hora
   - CRITICAL: serviço de inferência down ou latência p95 > 500ms
   - WARNING: data drift detectado (PSI > 0.2 em qualquer feature)
   - WARNING: quality check falhou em mais de 5% dos registros
   - WARNING: modelo em produção há mais de 30 dias sem retreino
   - INFO: volume de transações 50% abaixo da média (possível problema upstream)

5. **alertas/notification_channels.yaml** — Canais de notificação:
   - Slack webhook para canal #data-alerts
   - Email para equipe de plantão
   - PagerDuty para alertas CRITICAL

Crie um Makefile target: make monitoring-up (sobe a stack de monitoramento).
```

### Prompt 5.2 — Scripts de automação e operação

```
No diretório scripts/, crie automações para operação do dia-a-dia:

1. **ops/reprocess_partition.py** — Reprocessamento de dados:
   - Recebe: camada (bronze/silver/gold), data_inicio, data_fim
   - Deleta partições existentes no range
   - Reroda o job Spark para as datas especificadas
   - Valida resultado e loga métricas
   - Uso: python scripts/ops/reprocess_partition.py --layer silver --start 2024-03-01 --end 2024-03-07

2. **ops/health_check.py** — Health check completo do ambiente:
   - Verifica: Redpanda (tópicos, lag), S3 (buckets acessíveis), Glue Catalog (tabelas existem), Airflow (DAGs ativas, sem tasks falhando), MLflow (tracking server up), Feast (online store fresh), Inference API (health endpoint)
   - Output: relatório colorido no terminal com status de cada componente
   - Exit code 0 se tudo ok, 1 se algum problema
   - Uso: python scripts/ops/health_check.py --env dev --verbose

3. **ops/data_quality_report.py** — Relatório de qualidade:
   - Conecta nas tabelas Iceberg e gera estatísticas:
     - Row counts por dia (últimos 30 dias)
     - Null percentages por coluna
     - Distribuição de valores (min, max, mean, percentiles)
     - Freshness de cada tabela
   - Gera relatório HTML com gráficos (usa plotly ou matplotlib)
   - Pode ser enviado por email automaticamente

4. **ops/cleanup_orphan_files.py** — Limpeza de arquivos órfãos:
   - Lista arquivos no S3 que não estão referenciados por nenhuma tabela Iceberg
   - Mostra espaço a ser liberado
   - Flag --dry-run (padrão) vs --execute para deletar
   - Logging detalhado de tudo que foi removido

5. **ops/rotate_secrets.py** — Placeholder para rotação de credenciais:
   - Rotaciona credenciais do PostgreSQL
   - Atualiza secrets no AWS Secrets Manager
   - Reinicia serviços que dependem das credenciais

6. **ops/generate_runbook.py** — Gera runbook automaticamente:
   - Lê alert_rules.yml e gera um documento markdown com:
     - Cada alerta, sua severidade e threshold
     - Steps de troubleshooting para cada alerta
     - Comandos de diagnóstico
     - Contatos de escalação

Todos os scripts devem:
- Usar logging estruturado (JSON)
- Aceitar --env (dev/staging/prod) e --dry-run
- Ter docstring completo com exemplos de uso
- Retornar exit codes corretos (0=ok, 1=erro, 2=warning)
```

### Prompt 5.3 — Documentação final e runbook

```
No diretório docs/, crie a documentação completa do projeto:

1. **architecture.md** — Documentação de arquitetura:
   - Diagrama de arquitetura completo (use Mermaid para renderizar no GitHub)
   - Descrição de cada componente e sua responsabilidade
   - Fluxo de dados end-to-end (da transação no app até o score de anomalia)
   - Decisões arquiteturais (ADRs):
     a) Por que Redpanda vs Kafka?
     b) Por que Iceberg vs Delta Lake?
     c) Por que Feast vs alternativas?
     d) Por que Isolation Forest vs Deep Learning para anomalias?
   - Trade-offs e limitações conhecidas

2. **runbook.md** — Runbook operacional:
   Para cada cenário de falha, documente:
   - Sintoma (o que o alerta diz)
   - Diagnóstico (comandos para investigar)
   - Resolução (passo a passo)
   - Prevenção (como evitar no futuro)
   
   Cenários:
   a) Pipeline ETL falhando
   b) Consumer lag crescendo no Redpanda
   c) Debezium parou de capturar CDC
   d) Modelo de ML com drift detectado
   e) Tabela Iceberg corrompida
   f) S3 bucket ficou inacessível
   g) Airflow DAG travada
   h) Serviço de inferência com latência alta

3. **onboarding.md** — Guia para novos membros:
   - Pré-requisitos (Docker, Python, Terraform, AWS CLI)
   - Setup do ambiente local passo a passo
   - Como rodar o pipeline completo localmente
   - Como criar um novo job ETL
   - Como adicionar uma nova feature ao Feast
   - Como retreinar e deployar o modelo
   - Convenções de código e padrões do time

4. **data_catalog.md** — Catálogo de dados:
   - Descrição de cada tabela (bronze, silver, gold)
   - Schema completo com tipos e descrições de cada coluna
   - Linhagem de dados (de onde vem cada campo)
   - SLAs de freshness de cada tabela
   - Responsáveis (owners) de cada dataset

5. **monitoring_guide.md** — Guia de monitoramento:
   - Link e acesso para cada dashboard
   - O que cada painel mostra e como interpretar
   - Thresholds de alerta e o que significam
   - Como silenciar alertas durante manutenção

Toda documentação deve usar Mermaid para diagramas (renderiza nativamente no GitHub) e incluir exemplos de código/comandos reais do projeto.
```

---

## DICA FINAL — Prompt para revisão completa

```
Revise todo o projeto fx-data-platform e faça uma análise completa:

1. Verifique se todos os imports estão corretos e as dependências estão no pyproject.toml
2. Verifique se o docker-compose.yml sobe sem erros (docker compose config)
3. Rode todos os testes: pytest tests/ -v --tb=short
4. Verifique se os módulos Terraform são válidos: terraform validate
5. Verifique se as DAGs do Airflow não têm erros de import: python -c "from airflow.models import DagBag; d=DagBag('orchestration/dags/'); print(d.import_errors)"
6. Rode o linter: ruff check . --fix
7. Verifique se o README.md está atualizado com todas as instruções

Para cada problema encontrado, corrija e explique o que estava errado. Ao final, gere um relatório de "project readiness" com checklist de tudo que está ok e o que precisa de atenção.
```