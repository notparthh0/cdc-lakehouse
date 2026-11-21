"""Debezium topics -> append-only Iceberg changelog tables (cdc.<table>_changes), exactly-once.

Every Iceberg commit also records, in its snapshot summary, the next Kafka offset of each
partition it covers. Data and consumer position therefore move in one atomic commit. On start
the sink seeks to those offsets, so a crash at any point replays exactly the uncommitted events.
Kafka consumer-group commits are not used.

--naive runs the v1 design instead (Kafka auto-commit, offsets unrelated to Iceberg commits)
as the experiment's baseline.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone

import pyarrow as pa
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, TopicPartition
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.table.snapshots import ancestors_of

from cdc.config import KAFKA_BOOTSTRAP, TOPICS, WAREHOUSE

NAMESPACE = "cdc"
OFFSETS_KEY = "kafka.offsets"

PRIMITIVE = {"int8": pa.int32(), "int16": pa.int32(), "int32": pa.int32(), "int64": pa.int64(),
             "float32": pa.float32(), "float64": pa.float64(), "boolean": pa.bool_(), "string": pa.string()}
LOGICAL = {"io.debezium.time.MicroTimestamp": pa.timestamp("us")}  # anything else keeps its primitive type
META = [
    pa.field("_op", pa.string()),            # r = snapshot read, c = insert, u = update, d = delete
    pa.field("_lsn", pa.int64()),            # Postgres WAL position of the change: orders versions of a row
    pa.field("_tx_id", pa.int64()),
    pa.field("_source_ts", pa.timestamp("us")),  # commit time in Postgres (snapshot read time for r)
    pa.field("_kafka_partition", pa.int32()),
    pa.field("_kafka_offset", pa.int64()),
    pa.field("_ingested_at", pa.timestamp("us")),  # when this sink committed the row to Iceberg
]


def catalog() -> Catalog:
    os.makedirs(WAREHOUSE, exist_ok=True)
    cat = load_catalog("lakehouse", type="sql", uri=f"sqlite:///{WAREHOUSE}/catalog.db",
                       warehouse=f"file://{WAREHOUSE}")
    try:
        cat.create_namespace(NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    return cat


def table_id(topic: str) -> str:
    return f"{NAMESPACE}.{topic.rsplit('.', 1)[-1]}_changes"


def arrow_schema(envelope_schema: dict) -> pa.Schema:
    """Changelog schema from the Debezium envelope: CDC metadata + the row's columns, all nullable
    (delete events only carry the primary key)."""
    row = next(f for f in envelope_schema["fields"] if f["field"] == "after")
    cols = [pa.field(f["field"], LOGICAL.get(f.get("name"), PRIMITIVE[f["type"]])) for f in row["fields"]]
    return pa.schema(META + cols)


def to_row(payload: dict, partition: int, offset: int) -> dict:
    src = payload["source"]
    data = payload["before"] if payload["op"] == "d" else payload["after"]
    return {**data, "_op": payload["op"], "_lsn": src["lsn"], "_tx_id": src.get("txId"),
            "_source_ts": src["ts_us"], "_kafka_partition": partition, "_kafka_offset": offset}


def stored_offsets(cat: Catalog, topic: str) -> dict[int, int]:
    """Next offset per partition as of the sink's last commit ({} if nothing committed yet). Walks back past
    snapshots written by anything else (e.g. compaction), which carry no offsets."""
    try:
        tbl = cat.load_table(table_id(topic))
    except NoSuchTableError:
        return {}
    for snap in ancestors_of(tbl.current_snapshot(), tbl.metadata):
        if OFFSETS_KEY in snap.summary.additional_properties:
            return {int(p): o for p, o in json.loads(snap.summary.additional_properties[OFFSETS_KEY]).items()}
    return {}


def ensure_table(cat: Catalog, topic: str, schema: pa.Schema):
    """Create the table, or add any new source columns (ALTER TABLE ... ADD COLUMN upstream)."""
    try:
        tbl = cat.load_table(table_id(topic))
    except NoSuchTableError:
        return cat.create_table(table_id(topic), schema=schema)
    if set(schema.names) - set(tbl.schema().column_names):
        with tbl.update_schema() as update:
            update.union_by_name(schema)
        tbl = cat.load_table(table_id(topic))
    return tbl


def commit(tbl, rows: list[dict], offsets: dict[int, int]) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for r in rows:
        r["_ingested_at"] = now
    data = pa.Table.from_pylist(rows, schema=tbl.schema().as_arrow())
    tbl.append(data, snapshot_properties={OFFSETS_KEY: json.dumps(offsets)})


def write_status(path: str | None, status: dict) -> None:
    if path:
        with open(path + ".tmp", "w") as f:
            json.dump(status, f)
        os.replace(path + ".tmp", path)


def wait_for_topics(consumer: Consumer) -> dict[str, list[int]]:
    while True:
        meta = consumer.list_topics(timeout=10).topics
        if all(t in meta for t in TOPICS):
            return {t: sorted(meta[t].partitions) for t in TOPICS}
        print("waiting for Debezium topics ...", flush=True)
        time.sleep(2)


def run(naive=False, flush_rows=5000, flush_secs=1.0, until_idle=None, status_path=None):
    cat = catalog()
    consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "iceberg-sink-naive" if naive else "iceberg-sink",
                         "enable.auto.commit": naive,
                         # exactly-once: a stored offset Kafka has already deleted (retention) must fail loudly,
                         # not jump silently to the oldest message and skip the gap
                         "auto.offset.reset": "earliest" if naive else "error"})
    partitions = wait_for_topics(consumer)
    positions = {t: {} if naive else stored_offsets(cat, t) for t in TOPICS}
    if naive:
        consumer.subscribe(list(TOPICS))
    else:
        consumer.assign([TopicPartition(t, p, positions[t].get(p, OFFSET_BEGINNING))
                         for t in TOPICS for p in partitions[t]])
        print(f"resuming from Iceberg-stored offsets {positions}", flush=True)

    tables, schemas, buffers = {}, {}, {t: [] for t in TOPICS}
    polled = committed = 0
    last_flush = last_msg = time.monotonic()

    def flush():
        nonlocal committed, last_flush
        for t, rows in buffers.items():
            if rows:
                commit(tables[t], rows, positions[t])
                committed += len(rows)
                buffers[t] = []
        last_flush = time.monotonic()

    try:
        while True:
            msgs = consumer.consume(num_messages=1000, timeout=0.2)
            for m in msgs:
                if m.error():
                    raise KafkaException(m.error())
                t = m.topic()
                if m.value() is not None:  # None = tombstone (disabled in the connector)
                    env = json.loads(m.value())
                    if schemas.get(t) != env["schema"]:
                        if buffers[t]:  # buffered rows were built for the old schema: commit them first
                            flush()
                        schemas[t] = env["schema"]
                        tables[t] = ensure_table(cat, t, arrow_schema(env["schema"]))
                    buffers[t].append(to_row(env["payload"], m.partition(), m.offset()))
                    polled += 1
                # only after the row is buffered, so a commit never covers an offset it does not hold
                positions[t][m.partition()] = m.offset() + 1
            now = time.monotonic()
            if msgs or (naive and not consumer.assignment()):
                last_msg = now  # naive: after a kill the group may take ~45 s to hand partitions back
            if sum(map(len, buffers.values())) >= flush_rows or now - last_flush >= flush_secs:
                flush()
            write_status(status_path, {"pid": os.getpid(), "polled": polled, "committed": committed})
            if until_idle and now - last_msg > until_idle:
                print(f"idle for {until_idle}s, exiting", flush=True)
                break
    except KeyboardInterrupt:
        pass
    finally:
        flush()
        consumer.close()
        write_status(status_path, {"pid": os.getpid(), "polled": polled, "committed": committed, "exited": True})
        print(f"polled {polled}, committed {committed}", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--naive", action="store_true", help="v1 design: Kafka auto-commit (baseline)")
    ap.add_argument("--flush-rows", type=int, default=5000)
    ap.add_argument("--flush-secs", type=float, default=1.0)
    ap.add_argument("--until-idle", type=float, help="exit after this many seconds without messages")
    ap.add_argument("--status", help="write progress JSON here (used by the experiment)")
    a = ap.parse_args()
    run(a.naive, a.flush_rows, a.flush_secs, a.until_idle, a.status)
