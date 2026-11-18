#!/bin/bash
# ============================================================
# Full setup script — builds and starts everything
# Author: Parth Alagh (02316401523)
# ============================================================

set -e

USE_MINIO="${USE_MINIO:-false}"

echo "============================================================"
echo "  CDC Lakehouse Pipeline — Full Setup"
echo "  PostgreSQL → Debezium → Kafka → Iceberg"
if [ "$USE_MINIO" = "true" ]; then
  echo "  Storage: MinIO (S3-compatible)"
else
  echo "  Storage: Local filesystem (./consumer/warehouse)"
fi
echo "============================================================"
echo ""

# Step 1: Start core containers
if [ "$USE_MINIO" = "true" ]; then
  echo "🚀 Step 1: Starting containers (with MinIO)..."
  docker compose up -d zookeeper kafka postgres minio
else
  echo "🚀 Step 1: Starting containers (no MinIO)..."
  docker compose up -d zookeeper kafka postgres
fi
echo "   ✅ Core containers started"
echo ""

# Step 2: Wait for services
echo "⏳ Step 2: Waiting for services to be ready..."

echo "   Waiting for Kafka..."
until docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    sleep 3
done
echo "   ✅ Kafka ready"

echo "   Waiting for PostgreSQL..."
until docker exec postgres pg_isready -U cdc_user > /dev/null 2>&1; do
    sleep 2
done
echo "   ✅ PostgreSQL ready"

if [ "$USE_MINIO" = "true" ]; then
  echo "   Waiting for MinIO..."
  until curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; do
      sleep 2
  done
  echo "   ✅ MinIO ready"

  echo "🪣 Step 2b: Creating MinIO bucket..."
  python3 -c "
import boto3
from botocore.client import Config
s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)
try:
    s3.create_bucket(Bucket='lakehouse-bucket')
    print('   ✅ Bucket lakehouse-bucket created')
except Exception as e:
    if 'BucketAlreadyOwnedByYou' in str(e):
        print('   ✅ Bucket already exists')
    else:
        print(f'   ⚠️  {e}')
"
fi
echo ""

# Step 3: Start Kafka Connect + UI
echo "🔌 Step 3: Starting Kafka Connect and Kafka UI..."
docker compose up -d kafka-connect kafka-ui
echo "   ✅ kafka-connect container started (warming up...)"
echo ""

# Step 4: Wait for Kafka Connect REST API
echo "⏳ Step 4: Waiting for Kafka Connect REST API (can take 2-3 min)..."
attempt=0
until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    attempt=$((attempt+1))
    if [ $attempt -ge 60 ]; then
        echo "   ❌ Kafka Connect did not start within 3 minutes. Check logs:"
        echo "      docker logs kafka-connect --tail 50"
        exit 1
    fi
    echo "   ...still waiting (attempt $attempt/60)"
    sleep 3
done
echo "   ✅ Kafka Connect ready"
echo ""

# Step 5: Register Debezium connector
echo "📡 Step 5: Registering Debezium source connector..."
bash "$(dirname "$0")/register-connector.sh"
echo ""

# Step 6: Wait for initial snapshot
echo "⏳ Step 6: Waiting for Debezium initial snapshot (15s)..."
sleep 15

# Step 7: Show topics
echo "📋 Step 7: Kafka topics created by Debezium:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep ecommerce || echo "   (connector still initializing — check: curl http://localhost:8083/connectors)"
echo ""

echo "============================================================"
echo "  ✅ Pipeline is READY!"
echo ""
echo "  Services running:"
echo "    Kafka UI:        http://localhost:8080"
echo "    Kafka Connect:   http://localhost:8083"
if [ "$USE_MINIO" = "true" ]; then
echo "    MinIO Console:   http://localhost:9001  (minioadmin/minioadmin)"
fi
echo "    PostgreSQL:      localhost:5433         (cdc_user/cdc_pass)"
echo ""
echo "  Next steps:"
echo "    1. Start Iceberg consumer (Terminal 1):"
echo "       cd consumer && source venv/bin/activate && python iceberg_consumer.py"
echo ""
echo "    2. Simulate database changes (Terminal 2):"
echo "       source consumer/venv/bin/activate && python scripts/simulate_changes.py"
echo ""
echo "    3. Query Iceberg tables:"
echo "       source consumer/venv/bin/activate && python scripts/query_iceberg.py"
echo "============================================================"
