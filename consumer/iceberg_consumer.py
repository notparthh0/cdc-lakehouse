"""
CDC Lakehouse — Iceberg Consumer
Consumes Debezium CDC events from Kafka topics,
applies change-data-capture logic, and writes
Apache Iceberg tables to S3-compatible storage (MinIO / GCS).

Author: Parth Alagh (02316401523)
"""

from __future__ import annotations

import json
import time
import signal
import sys
from datetime import datetime

from ai_analyzer import CDCAIAnalyzer

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, LongType,
    DoubleType, TimestampType, BooleanType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder

# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_GROUP_ID  = "iceberg-sink-consumer"
KAFKA_TOPICS    = [
    "ecommerce.public.customers",
    "ecommerce.public.products",
    "ecommerce.public.orders"
]

import os as _os

# Storage backend — auto-detected from environment
# Set USE_MINIO=true to use MinIO (S3-compatible), otherwise uses local disk
_USE_MINIO = _os.getenv("USE_MINIO", "false").lower() == "true"

if _USE_MINIO:
    # MinIO / S3-compatible storage (cloud-ready)
    CATALOG_CONFIG = {
        "uri":                  "sqlite:///warehouse/catalog.db",
        "warehouse":            "s3://lakehouse-bucket/warehouse",
        "s3.endpoint":          _os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        "s3.access-key-id":     _os.getenv("MINIO_USER", "minioadmin"),
        "s3.secret-access-key": _os.getenv("MINIO_PASSWORD", "minioadmin"),
        "s3.region":            "us-east-1",
    }
    print("🗄️  Storage: MinIO (S3-compatible)")
else:
    # Local filesystem — no MinIO needed (great for local dev & demos)
    _warehouse_path = _os.path.abspath("warehouse")
    CATALOG_CONFIG = {
        "uri":       f"sqlite:///{_warehouse_path}/catalog.db",
        "warehouse": f"file://{_warehouse_path}",
    }
    print(f"🗄️  Storage: Local filesystem ({_warehouse_path})")

NAMESPACE   = "cdc"
FLUSH_EVERY = 50       # flush to Iceberg every N events
FLUSH_INTERVAL = 10    # or every N seconds

# ============================================================
# Iceberg Schemas (match PostgreSQL source tables)
# ============================================================

CDC_META_FIELDS = [
    NestedField(field_id=100, name="__op",         type=StringType(),    required=False),
    NestedField(field_id=101, name="__source_ts",   type=LongType(),      required=False),
    NestedField(field_id=102, name="__ingested_at",  type=TimestampType(), required=False),
    NestedField(field_id=103, name="__deleted",      type=BooleanType(),   required=False),
]

SCHEMAS = {
    "customers": Schema(
        NestedField(field_id=1, name="id",         type=IntegerType(), required=True),
        NestedField(field_id=2, name="name",       type=StringType(),  required=False),
        NestedField(field_id=3, name="email",      type=StringType(),  required=False),
        NestedField(field_id=4, name="city",       type=StringType(),  required=False),
        NestedField(field_id=5, name="created_at", type=LongType(),    required=False),
        NestedField(field_id=6, name="updated_at", type=LongType(),    required=False),
        *CDC_META_FIELDS,
    ),
    "products": Schema(
        NestedField(field_id=1, name="id",         type=IntegerType(), required=True),
        NestedField(field_id=2, name="name",       type=StringType(),  required=False),
        NestedField(field_id=3, name="category",   type=StringType(),  required=False),
        NestedField(field_id=4, name="price",      type=DoubleType(),  required=False),
        NestedField(field_id=5, name="stock_qty",  type=IntegerType(), required=False),
        NestedField(field_id=6, name="created_at", type=LongType(),    required=False),
        NestedField(field_id=7, name="updated_at", type=LongType(),    required=False),
        *CDC_META_FIELDS,
    ),
    "orders": Schema(
        NestedField(field_id=1,  name="id",           type=IntegerType(), required=True),
        NestedField(field_id=2,  name="customer_id",  type=IntegerType(), required=False),
        NestedField(field_id=3,  name="product_id",   type=IntegerType(), required=False),
        NestedField(field_id=4,  name="quantity",      type=IntegerType(), required=False),
        NestedField(field_id=5,  name="total_amount",  type=DoubleType(),  required=False),
        NestedField(field_id=6,  name="status",        type=StringType(),  required=False),
        NestedField(field_id=7,  name="order_date",    type=LongType(),    required=False),
        NestedField(field_id=8,  name="updated_at",    type=LongType(),    required=False),
        *CDC_META_FIELDS,
    ),
}

# Map Kafka topic suffix → Iceberg table name
def topic_to_table(topic: str) -> str:
    """ecommerce.public.customers → customers"""
    return topic.split(".")[-1]

# ============================================================
# Iceberg Catalog & Table Setup
# ============================================================

def init_catalog():
    """Initialise PyIceberg SQL catalog pointing to MinIO/GCS."""
    import os
    os.makedirs("warehouse", exist_ok=True)

    catalog = SqlCatalog("lakehouse", **CATALOG_CONFIG)

    # Create namespace if not exists
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"✅ Created namespace '{NAMESPACE}'")
    except Exception:
        print(f"📁 Namespace '{NAMESPACE}' already exists")

    # Create tables if not exist
    for table_name, schema in SCHEMAS.items():
        full_name = f"{NAMESPACE}.{table_name}"
        try:
            catalog.create_table(full_name, schema=schema)
            print(f"✅ Created Iceberg table '{full_name}'")
        except Exception:
            print(f"📁 Table '{full_name}' already exists")

    return catalog


# ============================================================
# CDC Event Parsing
# ============================================================

def parse_debezium_event(msg_value: dict, table_name: str) -> dict | None:
    """
    Parse a Debezium CDC envelope into a flat row dict.

    Debezium format:
    {
      "before": {...},
      "after":  {...},
      "op": "c|u|d|r",
      "ts_ms": 1234567890,
      "source": {...}
    }
    """
    if msg_value is None:
        return None

    op = msg_value.get("op")           # c=create, u=update, d=delete, r=read(snapshot)
    ts = msg_value.get("ts_ms", 0)

    if op in ("c", "r", "u"):
        payload = msg_value.get("after", {})
        if payload is None:
            return None
        row = dict(payload)
    elif op == "d":
        payload = msg_value.get("before", {})
        if payload is None:
            return None
        row = dict(payload)
    else:
        return None

    # Add CDC metadata columns
    row["__op"]          = op
    row["__source_ts"]   = ts
    row["__ingested_at"] = datetime.utcnow()
    row["__deleted"]     = (op == "d")

    return row


# ============================================================
# Flush Buffer → Iceberg
# ============================================================

def flush_to_iceberg(catalog, buffers: dict):
    """Write buffered rows to their respective Iceberg tables."""
    for table_name, rows in buffers.items():
        if not rows:
            continue

        full_name = f"{NAMESPACE}.{table_name}"
        table = catalog.load_table(full_name)

        # Build PyArrow table from rows
        columns = {}
        for key in rows[0].keys():
            columns[key] = [r.get(key) for r in rows]

        arrow_table = pa.table(columns)

        # Append to Iceberg table
        table.append(arrow_table)
        print(f"   💾 Flushed {len(rows)} rows → {full_name}")

    # Clear all buffers
    for key in buffers:
        buffers[key] = []


# ============================================================
# Main Consumer Loop
# ============================================================

def run_consumer():
    catalog = init_catalog()
    analyzer = CDCAIAnalyzer()   # AI anomaly analyzer

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           KAFKA_GROUP_ID,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(KAFKA_TOPICS)
    print(f"\n🔄 Consuming from: {KAFKA_TOPICS}")
    print(f"   Flush every {FLUSH_EVERY} events or {FLUSH_INTERVAL}s\n")

    # Buffers: table_name → [row_dicts]
    buffers = {t: [] for t in SCHEMAS.keys()}
    total_count = 0
    last_flush  = time.time()

    # Graceful shutdown
    running = True
    def shutdown(sig, frame):
        nonlocal running
        print("\n🛑 Shutting down... flushing remaining events")
        running = False
    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Check time-based flush
                if time.time() - last_flush >= FLUSH_INTERVAL and any(buffers.values()):
                    print(f"⏰ Time-based flush ({FLUSH_INTERVAL}s)")
                    flush_to_iceberg(catalog, buffers)
                    last_flush = time.time()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"❌ Kafka error: {msg.error()}")
                continue

            # Parse the CDC event
            topic      = msg.topic()
            table_name = topic_to_table(topic)
            value      = json.loads(msg.value().decode("utf-8"))

            row = parse_debezium_event(value, table_name)
            if row is None:
                continue

            buffers[table_name].append(row)
            total_count += 1

            op_label = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}.get(row["__op"], "?")
            print(f"   📥 [{op_label}] {table_name}.id={row.get('id')} (buffer: {sum(len(v) for v in buffers.values())})")

            # AI anomaly analysis (skips snapshot events to avoid noise)
            if row["__op"] != "r":
                analyzer.analyze(table_name, row, row["__op"])

            # Count-based flush
            buffered = sum(len(v) for v in buffers.values())
            if buffered >= FLUSH_EVERY:
                print(f"📦 Count-based flush ({buffered} events)")
                flush_to_iceberg(catalog, buffers)
                last_flush = time.time()

    finally:
        # Final flush
        if any(buffers.values()):
            print("📦 Final flush...")
            flush_to_iceberg(catalog, buffers)
        consumer.close()
        print(f"\n✅ Done. Total events processed: {total_count}")


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("  CDC Lakehouse — Iceberg Consumer")
    print("  PostgreSQL → Debezium → Kafka → Iceberg (MinIO/GCS)")
    print("=" * 60)
    run_consumer()
