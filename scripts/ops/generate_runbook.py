"""FX Data Platform - Runbook Generator.

Reads alert_rules.yml and generates a markdown runbook document with
troubleshooting steps, diagnostic commands, and escalation contacts
for each alert.

Usage:
    python scripts/ops/generate_runbook.py --env dev
    python scripts/ops/generate_runbook.py --env dev --output docs/runbook_generated.md
    python scripts/ops/generate_runbook.py --env dev --dry-run
"""

import sys
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path

import structlog
import yaml

logger = structlog.get_logger(__name__)

ALERT_RULES_PATH = "monitoring/prometheus/alert_rules.yml"

# Troubleshooting knowledge base for each alert
TROUBLESHOOTING = {
    "HighConsumerLag": {
        "description": "Consumer lag no Redpanda esta acima do threshold, indicando que consumidores nao estao processando mensagens rapido o suficiente.",
        "diagnosis": [
            "docker compose exec redpanda rpk group list",
            "docker compose exec redpanda rpk group describe <consumer-group>",
            "docker compose logs redpanda-connect --tail 100",
            "curl -s http://localhost:9644/public_metrics | grep consumer_lag",
        ],
        "resolution": [
            "1. Verificar se o Redpanda Connect esta rodando: `docker compose ps redpanda-connect`",
            "2. Verificar logs do consumer: `docker compose logs redpanda-connect --tail 200`",
            "3. Se consumer crashou, reiniciar: `docker compose restart redpanda-connect`",
            "4. Se lag continua alto, verificar se MinIO esta acessivel (sink do pipeline)",
            "5. Em caso de burst, considerar escalar consumidores ou aumentar batch size",
        ],
        "prevention": [
            "Monitorar tendencia de lag antes de atingir threshold critico",
            "Configurar auto-scaling de consumidores baseado em lag",
            "Revisar batch_size e linger_ms do consumer periodicamente",
        ],
    },
    "NoDataIngestion": {
        "description": "Nenhum dado novo foi gravado na camada raw ha mais de 1 hora. Pipeline de ingestao pode estar parado.",
        "diagnosis": [
            "docker compose ps  # verificar todos os servicos",
            "docker compose logs streaming-producer --tail 50",
            "docker compose logs redpanda-connect --tail 50",
            "docker compose exec redpanda rpk topic consume fx.transactions --num 1",
            "python -m ingestion.consumer.health_check_consumer --duration 10",
        ],
        "resolution": [
            "1. Verificar se produtores estao rodando: `docker compose ps`",
            "2. Verificar se Redpanda esta acessivel: `docker compose exec redpanda rpk cluster health`",
            "3. Verificar se Redpanda Connect esta gravando no MinIO: `docker compose logs redpanda-connect`",
            "4. Verificar se MinIO tem espaco: `curl http://localhost:9000/minio/health/live`",
            "5. Se PostgreSQL parou o CDC: `docker compose logs postgres | grep replication`",
            "6. Reiniciar pipeline: `docker compose restart redpanda-connect`",
        ],
        "prevention": [
            "Healthchecks automaticos a cada 15min (dag_streaming_monitor)",
            "Alertas de consumer lag como indicador antecipado",
            "Monitorar espaco em disco do MinIO",
        ],
    },
    "InferenceServiceCritical": {
        "description": "Servico de inferencia esta down ou com latencia p95 acima de 500ms.",
        "diagnosis": [
            "curl -s http://localhost:8000/health | python -m json.tool",
            "curl -s http://localhost:8000/metrics",
            "docker compose logs inference-service --tail 100",
            "docker stats fx-inference-service  # verificar CPU/memoria",
        ],
        "resolution": [
            "1. Verificar health: `curl http://localhost:8000/health`",
            "2. Se down, reiniciar: `docker compose restart inference-service`",
            "3. Se latencia alta, verificar se modelo esta carregado nos logs",
            "4. Verificar se Feast online store esta acessivel",
            "5. Verificar se MLflow esta up (modelo carrega de la): `curl http://localhost:5000`",
            "6. Se memoria alta, pode ser leak - reiniciar e monitorar",
        ],
        "prevention": [
            "Load tests regulares com locust",
            "Monitorar tendencia de latencia antes de atingir threshold",
            "Configurar limites de memoria no container",
        ],
    },
    "DataDriftDetected": {
        "description": "PSI (Population Stability Index) de uma ou mais features excede 0.2, indicando mudanca significativa na distribuicao dos dados de entrada.",
        "diagnosis": [
            "curl -s http://localhost:8000/metrics | grep drift",
            "python -m ml.feature_store.scripts.validate_features --env dev",
            "docker compose logs inference-service --tail 50 | grep drift",
        ],
        "resolution": [
            "1. Identificar quais features mudaram: verificar metricas de PSI por feature",
            "2. Analisar se mudanca e esperada (sazonalidade, novo produto, etc.)",
            "3. Se inesperada, verificar dados de origem por anomalias",
            "4. Considerar retreino do modelo: `make ml-train`",
            "5. Se retreino necessario, seguir pipeline: train -> evaluate -> promote",
        ],
        "prevention": [
            "Retreino regular do modelo (semanal via dag_ml_pipeline)",
            "Monitorar distribuicao de features continuamente",
            "Manter dataset de referencia atualizado",
        ],
    },
    "ModelStaleness": {
        "description": "Modelo em producao nao e retreinado ha mais de 30 dias. Performance pode estar degradando.",
        "diagnosis": [
            "curl -s http://localhost:5000/api/2.0/mlflow/registered-models/get?name=fx-anomaly-detector",
            "curl -s http://localhost:8000/health | python -m json.tool  # verificar model_version",
            "docker compose logs inference-service | grep 'model version'",
        ],
        "resolution": [
            "1. Verificar ultimo treino no MLflow: http://localhost:5000",
            "2. Rodar pipeline de retreino: `python -m ml.training.train_anomaly_detector --env dev`",
            "3. Avaliar modelo: `python -m ml.training.evaluate_model --env dev`",
            "4. Promover se aprovado: `python -m ml.training.promote_model --env dev`",
            "5. Verificar que inference service carregou nova versao",
        ],
        "prevention": [
            "Pipeline automatico semanal (dag_ml_pipeline)",
            "Monitorar metricas de performance do modelo continuamente",
        ],
    },
    "DataQualityDegraded": {
        "description": "Mais de 5% dos registros estao falhando nos quality checks em uma camada.",
        "diagnosis": [
            "python scripts/ops/data_quality_report.py --env dev",
            "docker compose logs airflow --tail 100 | grep quality",
            "# Verificar ultimos quality reports no Airflow UI: http://localhost:8090",
        ],
        "resolution": [
            "1. Gerar relatorio de qualidade: `python scripts/ops/data_quality_report.py --env dev`",
            "2. Identificar quais checks estao falhando e em quais colunas",
            "3. Verificar dados de origem (raw) por problemas",
            "4. Se problema na origem, verificar produtores e CDC",
            "5. Se problema na transformacao, verificar job Spark correspondente",
            "6. Reprocessar particoes afetadas: `python scripts/ops/reprocess_partition.py --layer silver --start <date> --end <date> --execute`",
        ],
        "prevention": [
            "Quality checks em cada camada do ETL",
            "Alertas antecipados quando quality degrada gradualmente",
            "Schema enforcement na ingestao",
        ],
    },
    "LowTransactionVolume": {
        "description": "Volume de transacoes esta 50% ou mais abaixo da media historica.",
        "diagnosis": [
            "docker compose exec redpanda rpk topic consume fx.transactions --num 1",
            "docker compose logs streaming-producer --tail 50",
            "# Verificar se e horario de baixo volume (noite, fim de semana)",
            "curl -s http://localhost:9644/public_metrics | grep produce",
        ],
        "resolution": [
            "1. Verificar se e queda esperada (horario, dia da semana, feriado)",
            "2. Se inesperado, verificar se produtores estao rodando",
            "3. Verificar se PostgreSQL CDC esta enviando dados",
            "4. Verificar se ha problemas no sistema upstream",
            "5. Se confirmado problema, escalar para equipe responsavel",
        ],
        "prevention": [
            "Baselines de volume por horario e dia da semana",
            "Alertas com thresholds adaptativos",
        ],
    },
    "ServiceDown": {
        "description": "Um servico da plataforma esta inacessivel.",
        "diagnosis": [
            "docker compose ps",
            "docker compose logs <service> --tail 100",
            "docker stats",
            "python scripts/ops/health_check.py --env dev --verbose",
        ],
        "resolution": [
            "1. Identificar qual servico esta down: `docker compose ps`",
            "2. Verificar logs: `docker compose logs <service> --tail 200`",
            "3. Reiniciar servico: `docker compose restart <service>`",
            "4. Se nao iniciar, verificar recursos (disco, memoria): `docker stats`",
            "5. Se problema persiste, verificar configuracao e dependencias",
        ],
        "prevention": [
            "Healthchecks em todos os containers",
            "restart: unless-stopped em todos os servicos",
            "Monitoramento de recursos do host",
        ],
    },
    "HighCPUUsage": {
        "description": "Uso de CPU acima de 80% por mais de 10 minutos.",
        "diagnosis": [
            "docker stats --no-stream",
            "top -bn1 | head -20",
            "docker compose logs --tail 50",
        ],
        "resolution": [
            "1. Identificar container consumindo mais CPU: `docker stats --no-stream`",
            "2. Se Spark job, pode ser processamento pesado - verificar se e esperado",
            "3. Se inference service, pode ser burst de requests - verificar rate",
            "4. Se Redpanda, verificar throughput e consumer groups",
            "5. Considerar limitar CPU por container no docker-compose.yml",
        ],
        "prevention": [
            "Limites de CPU por container",
            "Auto-scaling em producao",
            "Otimizar queries Spark pesadas",
        ],
    },
    "LowDiskSpace": {
        "description": "Menos de 10% de espaco em disco disponivel.",
        "diagnosis": [
            "df -h",
            "docker system df",
            "du -sh /var/lib/docker/volumes/*",
        ],
        "resolution": [
            "1. Verificar uso de disco: `df -h`",
            "2. Limpar imagens Docker nao usadas: `docker system prune -f`",
            "3. Limpar volumes orfaos: `docker volume prune -f`",
            "4. Rodar cleanup de arquivos orfaos: `python scripts/ops/cleanup_orphan_files.py --execute`",
            "5. Rodar manutencao Iceberg: expire snapshots e remove orphan files",
            "6. Se necessario, expandir disco",
        ],
        "prevention": [
            "Lifecycle policies no S3 (raw -> Glacier apos 90 dias)",
            "Manutencao Iceberg regular (expire snapshots, compaction)",
            "Monitoramento proativo de espaco em disco",
        ],
    },
}

ESCALATION_CONTACTS = {
    "critical": {
        "primary": "Data Engineering On-Call (PagerDuty)",
        "secondary": "Tech Lead - data-team@company.com",
        "channel": "#data-alerts (Slack)",
    },
    "warning": {
        "primary": "Data Engineering Team",
        "channel": "#data-alerts (Slack)",
    },
    "info": {
        "channel": "#data-alerts (Slack)",
    },
}


def load_alert_rules(path: str) -> list[dict]:
    """Load and parse alert rules from YAML file."""
    rules_path = Path(path)
    if not rules_path.exists():
        logger.error("Alert rules file not found", path=path)
        return []

    with open(rules_path) as f:
        data = yaml.safe_load(f)

    alerts = []
    for group in data.get("groups", []):
        group_name = group.get("name", "unknown")
        for rule in group.get("rules", []):
            alerts.append({
                "group": group_name,
                "name": rule.get("alert", "unknown"),
                "expr": rule.get("expr", ""),
                "for": rule.get("for", ""),
                "severity": rule.get("labels", {}).get("severity", "unknown"),
                "team": rule.get("labels", {}).get("team", "unknown"),
                "summary": rule.get("annotations", {}).get("summary", ""),
                "description": rule.get("annotations", {}).get("description", ""),
                "runbook_url": rule.get("annotations", {}).get("runbook_url", ""),
            })

    return alerts


def generate_runbook_markdown(alerts: list[dict]) -> str:
    """Generate markdown runbook from alerts and troubleshooting knowledge base."""
    lines = [
        "# FX Data Platform - Operational Runbook",
        "",
        f"> Auto-generated on {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        f"> Source: `{ALERT_RULES_PATH}`",
        "",
        "## Table of Contents",
        "",
    ]

    # TOC
    for alert in alerts:
        severity_emoji = {"critical": "🔴", "warning": "🟡", "info": "🔵"}.get(alert["severity"], "⚪")
        lines.append(f"- [{severity_emoji} {alert['name']}](#{alert['name'].lower()}) ({alert['severity'].upper()})")

    lines.extend(["", "---", ""])

    # Escalation contacts
    lines.extend([
        "## Escalation Contacts",
        "",
        "| Severity | Primary | Secondary | Channel |",
        "|----------|---------|-----------|---------|",
    ])
    for severity, contacts in ESCALATION_CONTACTS.items():
        lines.append(
            f"| {severity.upper()} | {contacts.get('primary', '-')} | "
            f"{contacts.get('secondary', '-')} | {contacts.get('channel', '-')} |"
        )
    lines.extend(["", "---", ""])

    # Alert details
    for alert in alerts:
        name = alert["name"]
        severity = alert["severity"].upper()
        troubleshooting = TROUBLESHOOTING.get(name, {})

        lines.extend([
            f"## {name}",
            "",
            f"**Severity:** {severity} | **Team:** {alert['team']} | **Group:** {alert['group']}",
            "",
        ])

        # Description
        desc = troubleshooting.get("description", alert.get("description", ""))
        if desc:
            lines.extend(["### Sintoma", "", desc, ""])

        # Alert expression
        lines.extend([
            "### Regra de Alerta",
            "",
            "```promql",
            f"{alert['expr']}",
            "```",
            f"**Threshold:** dispara apos {alert['for']}",
            "",
        ])

        # Diagnosis
        diagnosis = troubleshooting.get("diagnosis", [])
        if diagnosis:
            lines.extend(["### Diagnostico", "", "```bash"])
            for cmd in diagnosis:
                lines.append(cmd)
            lines.extend(["```", ""])

        # Resolution
        resolution = troubleshooting.get("resolution", [])
        if resolution:
            lines.extend(["### Resolucao", ""])
            for step in resolution:
                lines.append(step)
            lines.append("")

        # Prevention
        prevention = troubleshooting.get("prevention", [])
        if prevention:
            lines.extend(["### Prevencao", ""])
            for item in prevention:
                lines.append(f"- {item}")
            lines.append("")

        lines.extend(["---", ""])

    return "\n".join(lines)


def main() -> int:
    """Main entry point.

    Returns:
        0 if successful, 1 on error, 2 on warning.

    """
    parser = ArgumentParser(
        description="Generate operational runbook from alert rules"
    )
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment (default: dev)",
    )
    parser.add_argument(
        "--output",
        default="docs/runbook_generated.md",
        help="Output markdown file path (default: docs/runbook_generated.md)",
    )
    parser.add_argument(
        "--alert-rules",
        default=ALERT_RULES_PATH,
        help=f"Path to alert_rules.yml (default: {ALERT_RULES_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be generated without writing",
    )
    args = parser.parse_args()

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )

    logger.info(
        "Generating runbook",
        alert_rules=args.alert_rules,
        output=args.output,
        dry_run=args.dry_run,
    )

    alerts = load_alert_rules(args.alert_rules)
    if not alerts:
        logger.error("No alerts found in rules file")
        return 1

    logger.info("Loaded alerts", count=len(alerts))

    runbook = generate_runbook_markdown(alerts)

    if args.dry_run:
        print(runbook[:2000])
        print(f"\n... ({len(runbook)} chars total)")
        logger.info("DRY RUN - would write runbook", output=args.output, chars=len(runbook))
        return 0

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(runbook)
    logger.info("Runbook generated", path=args.output, alerts=len(alerts))

    return 0


if __name__ == "__main__":
    sys.exit(main())
