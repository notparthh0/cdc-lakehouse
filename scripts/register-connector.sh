#!/bin/bash
# ============================================================
# Register Debezium PostgreSQL source connector with Kafka Connect
# ============================================================

set -e

CONNECT_URL="http://localhost:8083"

echo "⏳ Waiting for Kafka Connect to be ready..."
until curl -s "$CONNECT_URL/connectors" > /dev/null 2>&1; do
    sleep 3
    echo "   Kafka Connect not ready yet..."
done
echo "✅ Kafka Connect is ready!"

echo ""
echo "📡 Registering Debezium PostgreSQL source connector..."
curl -s -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @"$(dirname "$0")/../connectors/source-postgres.json" | python3 -m json.tool

echo ""
echo "✅ Connector registered!"
echo ""

echo "📋 Active connectors:"
curl -s "$CONNECT_URL/connectors" | python3 -m json.tool

echo ""
echo "📊 Connector status:"
sleep 3
curl -s "$CONNECT_URL/connectors/postgres-source/status" | python3 -m json.tool
