"""Keep a pgvector semantic-search index of products fresh from the same CDC stream.

Own consumer group on the products topic. Per batch it keeps the latest event per product,
deletes removed products and re-embeds only products whose text (name, category, description)
changed, judged by a stored content hash, so price and stock updates cost no embedding.
Embeddings come from bge-small-en-v1.5 running locally (ONNX via fastembed): no API key, no quota.
Kafka offsets are committed after the pgvector transaction: at-least-once, and a replay is
harmless because every write is an idempotent upsert or delete by id.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import time

import psycopg
from confluent_kafka import Consumer, KafkaException

from cdc.config import KAFKA_BOOTSTRAP, PG_DSN, SEARCH_DSN

TOPIC = "ecommerce.public.products"
MODEL = "BAAI/bge-small-en-v1.5"  # 384 dimensions, matches vector(384) in postgres/init.sql


def content(row: dict) -> str:
    return f"{row['name']}. {row['category']}. {row['description']}"


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def plan(payloads: list[dict], indexed: dict[int, str]):
    """payloads in Kafka order; indexed = {id: content_hash} already in pgvector.
    Returns (to_embed [(id, text, hash, source_ts_us, op)], to_delete {id})."""
    latest = {}
    for p in payloads:  # all events of one product share a partition, so the last one wins
        latest[(p["before"] if p["op"] == "d" else p["after"])["id"]] = p
    to_embed, to_delete = [], set()
    for pid, p in latest.items():
        if p["op"] == "d":
            to_delete.add(pid)
            continue
        text = content(p["after"])
        if indexed.get(pid) != digest(text):
            to_embed.append((pid, text, digest(text), p["source"]["ts_us"], p["op"]))
    return to_embed, to_delete


def model():
    from fastembed import TextEmbedding
    return TextEmbedding(MODEL)


def vec(v) -> str:
    return "[" + ",".join(map(str, v)) + "]"


def write_status(path, stats, lags):
    if path:
        q = statistics.quantiles(lags, n=20) if len(lags) >= 2 else [None] * 19
        with open(path, "w") as f:
            json.dump({**stats, "lag_seconds_p50": q[9], "lag_seconds_p95": q[18]}, f)


def run(until_idle=None, status_path=None, embed=None):
    if embed is None:
        encoder = model()
        embed = lambda texts: list(encoder.embed(texts))  # noqa: E731
    db = psycopg.connect(SEARCH_DSN, autocommit=True)  # so db.transaction() below is a real COMMIT, not a savepoint
    consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "vector-sync",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    stats = {"events": 0, "embedded": 0, "deleted": 0}
    lags = []  # Postgres commit -> row in pgvector, per embedded change (snapshot rows excluded); one clock
    last_msg = time.monotonic()
    try:
        while True:
            msgs = consumer.consume(num_messages=500, timeout=1.0)
            payloads = []
            for m in msgs:
                if m.error():
                    raise KafkaException(m.error())
                if m.value() is not None:
                    payloads.append(json.loads(m.value())["payload"])
            if payloads:
                ids = [(p["before"] if p["op"] == "d" else p["after"])["id"] for p in payloads]
                indexed = dict(db.execute("SELECT id, content_hash FROM product_embeddings WHERE id = ANY(%s)",
                                          (ids,)).fetchall())
                to_embed, to_delete = plan(payloads, indexed)
                vectors = embed([t for _, t, _, _, _ in to_embed]) if to_embed else []
                with db.transaction():
                    for (pid, text, h, ts, op), v in zip(to_embed, vectors):
                        lag = db.execute(
                            "INSERT INTO product_embeddings (id, content, content_hash, embedding, source_ts, synced_at) "
                            "VALUES (%s, %s, %s, %s::vector, to_timestamp(%s / 1e6) AT TIME ZONE 'UTC', "
                            "clock_timestamp() AT TIME ZONE 'UTC') ON CONFLICT (id) DO UPDATE SET content = excluded.content, "
                            "content_hash = excluded.content_hash, embedding = excluded.embedding, "
                            "source_ts = excluded.source_ts, synced_at = excluded.synced_at "
                            "RETURNING extract(epoch FROM synced_at - source_ts)",
                            (pid, text, h, vec(v), ts)).fetchone()[0]
                        if op != "r" and status_path:  # only the experiment reads lags; don't grow forever
                            lags.append(float(lag))
                    if to_delete:
                        db.execute("DELETE FROM product_embeddings WHERE id = ANY(%s)", (list(to_delete),))
                consumer.commit(asynchronous=False)  # only after pgvector has the batch
                stats["events"] += len(payloads)
                stats["embedded"] += len(to_embed)
                stats["deleted"] += len(to_delete)
                write_status(status_path, stats, lags)
            if msgs:
                last_msg = time.monotonic()
            elif until_idle and time.monotonic() - last_msg > until_idle:
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        db.close()
        print(f"vector sync: {stats}", flush=True)
    return stats


def consistency() -> dict:
    """Compare the index with the source table: every live product indexed with its current text."""
    with psycopg.connect(PG_DSN) as src, psycopg.connect(SEARCH_DSN) as idx:
        want = {pid: digest(content({"name": n, "category": c, "description": d}))
                for pid, n, c, d in src.execute("SELECT id, name, category, description FROM products")}
        have = dict(idx.execute("SELECT id, content_hash FROM product_embeddings").fetchall())
    return {"products": len(want), "indexed": len(have),
            "missing": len(want.keys() - have.keys()), "orphans": len(have.keys() - want.keys()),
            "stale": sum(1 for k in want.keys() & have.keys() if want[k] != have[k])}


def search(query: str, k: int = 5):
    v = vec(next(iter(model().query_embed([query]))))
    with psycopg.connect(SEARCH_DSN) as db:
        rows = db.execute("SELECT id, content, round((1 - (embedding <=> %s::vector))::numeric, 3) "
                          "FROM product_embeddings ORDER BY embedding <=> %s::vector LIMIT %s", (v, v, k)).fetchall()
    for pid, text, score in rows:
        print(f"{score}  #{pid}  {text}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--search", help="run one semantic search and exit")
    ap.add_argument("--until-idle", type=float)
    ap.add_argument("--status")
    a = ap.parse_args()
    if a.search:
        search(a.search)
    else:
        run(a.until_idle, a.status)
