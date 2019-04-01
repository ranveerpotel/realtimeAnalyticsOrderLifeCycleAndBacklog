#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_FILE="${1:-postgres-connector.json}"

echo "Waiting for Kafka Connect to be ready..."
until curl -sf "${CONNECT_URL}/connectors" > /dev/null; do
    sleep 2
done

echo "Registering connector from ${CONNECTOR_FILE}..."
curl -X POST \
     -H "Content-Type: application/json" \
     --data "@${CONNECTOR_FILE}" \
     "${CONNECT_URL}/connectors"

echo ""
echo "Connector registered. Checking status..."
sleep 3
curl -sf "${CONNECT_URL}/connectors/orders-postgres-connector/status" | python3 -m json.tool
