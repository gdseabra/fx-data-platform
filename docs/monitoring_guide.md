# FX Data Platform — Monitoring Guide

Como usar os dashboards Grafana, interpretar metricas e gerenciar alertas.

---

## Acesso

| Servico | URL | Credenciais |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| AlertManager | http://localhost:9093 | - |

**Iniciar stack:** `make monitoring-up`
**Parar stack:** `make monitoring-down`

> A stack de monitoring roda separada do docker-compose principal. Precisa da rede `fx-network` criada pelo `make docker-up`.

---

## Dashboards

### 1. Pipeline Overview

**URL:** http://localhost:3000/d/fx-pipeline-overview

Dashboard principal com 4 grupos de paineis:

#### a) Pipeline Health
| Painel | O que mostra | Como interpretar |
|--------|-------------|------------------|
| Status UP/DOWN (6 paineis) | Health de cada servico | Verde = UP, Vermelho = DOWN. Se vermelho, verificar `docker compose ps` |
| Tempo desde ultimo ETL | Segundos desde ultima execucao bem-sucedida | Verde < 1h, Amarelo 1-2h, Vermelho > 2h (SLA breach) |
| SLA Compliance | % de runs dentro do SLA de 2h | Target: > 95%. Se abaixo, investigar jobs lentos |

#### b) Streaming Metrics
| Painel | O que mostra | Como interpretar |
|--------|-------------|------------------|
| Mensagens/seg por topico | Throughput de producao no Redpanda | Baseline ~100-500 msgs/s. Queda subita = problema no producer |
| Consumer Lag | Distancia entre producer e consumer | Verde < 1k, Amarelo 1k-10k, Vermelho > 10k |
| Tamanho das particoes | Storage por topico no Redpanda | Crescimento constante = normal. Picos = burst ou retencao |
| Taxa de erro | Erros/s no Redpanda Connect | Idealmente 0. Qualquer valor > 0 = investigar dead letter queue |

#### c) Data Quality
| Painel | O que mostra | Como interpretar |
|--------|-------------|------------------|
| Freshness por camada | Minutos desde ultima atualizacao | Raw < 30min, Bronze < 2h, Silver < 3h, Gold < 4h |
| Contagem de registros | Tendencia de volume por camada | Queda subita = problema na ingestao. Pico = burst |
| Quality Check Pass Rate | Gauge de % aprovacao | Verde > 95%, Amarelo 90-95%, Vermelho < 90% |
| Anomalias detectadas vs injetadas | Comparacao stacked bar | Barras devem ser proporcionais. Divergencia = modelo impreciso |

#### d) ML Model Performance
| Painel | O que mostra | Como interpretar |
|--------|-------------|------------------|
| Prediction Latency (p50/p95/p99) | Latencia do endpoint /predict | p95 target < 100ms. Se > 500ms = CRITICAL |
| Score Distribution | Histograma de anomaly scores | Maioria perto de 0 (normal). Pico em >0.7 = muitas anomalias |
| PSI por feature | Population Stability Index | Verde < 0.1, Amarelo 0.1-0.2, Vermelho > 0.2 (drift) |
| Model Version | Versao em producao | Informativo. Verificar se nao esta stale (> 30 dias) |
| F1 Score ao longo do tempo | Concept drift monitoring | Tendencia descendente = modelo perdendo performance |

---

### 2. Cost Tracking

**URL:** http://localhost:3000/d/fx-cost-tracking

| Painel | O que mostra | Como interpretar |
|--------|-------------|------------------|
| Storage por camada (S3) | Bytes por bucket no MinIO | Crescimento esperado. Raw maior que Gold |
| Objetos por bucket | Contagem de arquivos | Muitos arquivos pequenos = rodar Iceberg compaction |
| Custo estimado S3 (USD/mes) | Calculo baseado em $0.023/GB | Budget tracking. Alert se > threshold |
| Duracao dos jobs ETL | Segundos por job Spark | Baseline estavel. Aumento = mais dados ou regressao |
| Custo estimado EMR/Glue | Baseado em duracao * preco vCPU | Estimativa para planning de custos AWS |
| Throughput Redpanda | Bytes/seg por topico | Base para sizing de cluster Redpanda |

---

## Alertas

### Thresholds e significado

| Alerta | Severidade | Threshold | Significado |
|--------|-----------|-----------|-------------|
| `HighConsumerLag` | CRITICAL | lag > 10k por 5min | Consumidores nao acompanham producao |
| `NoDataIngestion` | CRITICAL | sem dados > 1h | Pipeline de ingestao parou |
| `InferenceServiceCritical` | CRITICAL | down ou p95 > 500ms | Servico de scoring indisponivel |
| `DataDriftDetected` | WARNING | PSI > 0.2 por 15min | Distribuicao de features mudou |
| `ModelStaleness` | WARNING | > 30 dias sem retreino | Modelo pode estar desatualizado |
| `DataQualityDegraded` | WARNING | > 5% registros falhando | Problemas nos dados de origem |
| `LowTransactionVolume` | INFO | 50% abaixo da media por 30min | Possivel problema upstream |
| `ServiceDown` | CRITICAL | qualquer servico inacessivel 3min | Servico caiu |
| `HighCPUUsage` | WARNING | CPU > 80% por 10min | Recursos do host sobrecarregados |
| `LowDiskSpace` | WARNING | disco < 10% por 10min | Risco de falha por falta de espaco |

### Routing de notificacoes

| Severidade | Canal | Tempo de resposta esperado |
|-----------|-------|---------------------------|
| CRITICAL | PagerDuty + Slack #data-alerts | < 15 minutos |
| WARNING | Slack #data-alerts + Email | Proximo dia util |
| INFO | Slack #data-alerts | Informativo apenas |

### Como verificar alertas ativos

```bash
# Via Prometheus
curl -s http://localhost:9090/api/v1/alerts | python -m json.tool

# Via AlertManager
curl -s http://localhost:9093/api/v2/alerts | python -m json.tool

# Via Grafana
# Menu lateral → Alerting → Alert rules
```

---

## Como silenciar alertas durante manutencao

### Via AlertManager API

```bash
# Silenciar todos os alertas por 2 horas
curl -X POST http://localhost:9093/api/v2/silences \
  -H "Content-Type: application/json" \
  -d '{
    "matchers": [{"name": "alertname", "value": ".*", "isRegex": true}],
    "startsAt": "'$(date -u +%Y-%m-%dT%H:%M:%S)'Z",
    "endsAt": "'$(date -u -d '+2 hours' +%Y-%m-%dT%H:%M:%S)'Z",
    "createdBy": "ops-team",
    "comment": "Manutencao planejada - ticket JIRA-123"
  }'

# Silenciar apenas um alerta especifico
curl -X POST http://localhost:9093/api/v2/silences \
  -H "Content-Type: application/json" \
  -d '{
    "matchers": [{"name": "alertname", "value": "HighConsumerLag", "isRegex": false}],
    "startsAt": "'$(date -u +%Y-%m-%dT%H:%M:%S)'Z",
    "endsAt": "'$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%S)'Z",
    "createdBy": "ops-team",
    "comment": "Rebalanceamento de consumers"
  }'

# Listar silences ativos
curl -s http://localhost:9093/api/v2/silences?filter=active | python -m json.tool

# Remover silence
curl -X DELETE http://localhost:9093/api/v2/silence/<silence-id>
```

### Via Grafana UI

1. Menu lateral → **Alerting** → **Silences**
2. Click **Create silence**
3. Definir matcher (ex: `alertname = HighConsumerLag`)
4. Definir duracao e comentario
5. **Create**

---

## Queries PromQL uteis

### Data Freshness

```promql
# Minutos desde ultima atualizacao por camada
(time() - fx_layer_last_update_timestamp) / 60
```

### Pipeline Performance

```promql
# SLA compliance (% de runs < 2h)
fx_pipeline_sla_compliance_ratio

# Duracao media do job ETL (ultimas 24h)
avg_over_time(fx_etl_job_duration_seconds[24h])
```

### Streaming

```promql
# Throughput do Redpanda (msgs/seg)
sum(rate(redpanda_kafka_request_produce_latency_seconds_count[5m])) by (redpanda_topic)

# Consumer lag maximo
max(redpanda_kafka_consumer_group_lag) by (group, topic)
```

### ML Model

```promql
# Latencia de inferencia p95
histogram_quantile(0.95, rate(fx_inference_request_latency_seconds_bucket[5m]))

# Distribuicao de risk levels
sum(fx_inference_requests_total) by (risk_level)

# PSI drift por feature
fx_feature_drift_psi
```

### Infrastructure

```promql
# CPU usage (%)
100 - (avg by(instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)

# Disco disponivel (%)
node_filesystem_avail_bytes{mountpoint="/"} / node_filesystem_size_bytes{mountpoint="/"} * 100

# Storage MinIO por bucket
minio_bucket_usage_total_bytes{bucket=~"fx-datalake-.*"}
```

---

## Como adicionar um alerta customizado

### 1. Editar regras do Prometheus

```bash
# Editar monitoring/prometheus/alert_rules.yml
vim monitoring/prometheus/alert_rules.yml
```

Adicionar nova regra:

```yaml
- alert: MyCustomAlert
  expr: 'my_metric > 100'
  for: 5m
  labels:
    severity: warning
    team: data-engineering
  annotations:
    summary: "Meu alerta customizado: {{ $value }}"
    description: "Descricao detalhada do problema"
    runbook_url: "https://link-para-runbook"
```

### 2. Recarregar Prometheus

```bash
# Hot reload (sem restart)
curl -X POST http://localhost:9090/-/reload

# Ou reiniciar container
docker compose -f docker-compose.monitoring.yml restart prometheus
```

### 3. Verificar que alerta foi carregado

```bash
curl -s http://localhost:9090/api/v1/rules | python -m json.tool | grep MyCustomAlert
```

### 4. Atualizar o runbook

```bash
python scripts/ops/generate_runbook.py --output docs/runbook_generated.md
```

---

## Troubleshooting do Monitoring

| Problema | Solucao |
|----------|---------|
| Dashboard "No data" | Verificar que Prometheus esta scraping: http://localhost:9090/targets |
| Alertas nao disparam | Verificar regras: http://localhost:9090/alerts. Verificar `for` clause |
| Grafana lento | Reduzir time range (24h em vez de 30d). Adicionar `rate()` |
| Prometheus sem espaco | Aumentar volume Docker ou reduzir retention (90d → 30d) |
| AlertManager nao envia | Verificar config: `curl http://localhost:9093/api/v2/status` |
| Targets "down" no Prometheus | Servicos da app devem estar na mesma rede Docker (`fx-network`) |

---

**Last Updated:** March 2026
**Next Review:** April 2026
