#!/bin/bash
# FX Data Platform - Register Debezium Connector Script
# Registers the CDC connector with Kafka Connect

set -e

KAFKA_CONNECT_HOST="${KAFKA_CONNECT_HOST:-localhost:8083}"
CONNECTOR_CONFIG="./connector-config.json"

echo "Registering Debezium CDC connector..."
echo "Kafka Connect: $KAFKA_CONNECT_HOST"

curl -X DELETE "http://$KAFKA_CONNECT_HOST/connectors/fx-cdc-connector" || true

sleep 2

curl -X POST \
  "http://$KAFKA_CONNECT_HOST/connectors" \
  -H "Content-Type: application/json" \
  -d @"$CONNECTOR_CONFIG"

echo ""
echo "✓ Connector registered successfully"
echo ""
echo "Check status with:"
echo "  curl http://$KAFKA_CONNECT_HOST/connectors/fx-cdc-connector/status"
