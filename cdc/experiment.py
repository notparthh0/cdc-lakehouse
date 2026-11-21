"""The measured run: OPS source changes at RATE ops/s, the sink killed with SIGKILL part-way, then checks.

All checks are against ground truth, never against the pipeline's own counters:
  * events: every change the workload made is in Iceberg exactly once (no missing, no duplicate rows)
  * state:  dbt's *_current tables equal the Postgres tables row for row, every column
  * index:  the pgvector index holds every live product with its current text (exactly-once mode)
--mode faults kills Kafka Connect (Debezium) and the vector sync instead of the sink, to test the
at-least-once parts: Debezium re-sends after a crash (dbt dedupes), the vector sync replays (idempotent).
Writes results/experiment_<mode>.json. Expects a fresh stack (make experiment runs reset + up first).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone

import duckdb
import psycopg

from cdc import workload
from cdc.config import PG_DSN, TABLES, WAREHOUSE
from cdc.sink import catalog
from cdc.vector_sync import consistency

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SINK_STATUS = os.path.join(WAREHOUSE, "sink_status.json")
VECTOR_STATUS = os.path.join(WAREHOUSE, "vector_status.json")
CHILDREN: list[subprocess.Popen] = []  # killed on exit so a failed run never leaves a second writer behind


def start(args: list[str], log) -> subprocess.Popen:
    # the Python process itself, not a make/shell wrapper, so SIGKILL hits the sink
    CHILDREN.append(subprocess.Popen([sys.executable, "-m", *args], stdout=log, stderr=subprocess.STDOUT, cwd=ROOT))
    return CHILDREN[-1]


def read_json(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def wait_until(cond, timeout: float, what: str) -> None:
    deadline = time.time() + timeout
    while not cond():
        if time.time() > deadline:
            raise TimeoutError(what)
        time.sleep(0.05)


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def changelogs() -> duckdb.DuckDBPyConnection:
    cat, con = catalog(), duckdb.connect()
    for t in TABLES:
        arrow = cat.load_table(f"cdc.{t}_changes").scan().to_arrow()
        con.register(f"{t}_changes", arrow)
    return con


def clock_skew() -> float:
    """Seconds the Postgres (Docker VM) clock is ahead of the host clock."""
    with psycopg.connect(PG_DSN) as c:
        t0 = time.time()
        pg = float(c.execute("SELECT extract(epoch FROM clock_timestamp())").fetchone()[0])
        t1 = time.time()
    return pg - (t0 + t1) / 2


def connector_running() -> bool:
    try:
        with urllib.request.urlopen("http://localhost:8083/connectors/ecommerce-source/status", timeout=2) as r:
            status = json.load(r)
    except (OSError, ValueError):  # Connect still starting: refused, reset, 404
        return False
    tasks = status.get("tasks", [])
    return status["connector"]["state"] == "RUNNING" and bool(tasks) and all(t["state"] == "RUNNING" for t in tasks)


def main(ops: int, rate: float, mode: str, kill_at: float, seed: int) -> dict:
    naive, faults = mode == "naive", mode == "faults"
    os.makedirs(WAREHOUSE, exist_ok=True)
    log = open(os.path.join(WAREHOUSE, f"experiment_{mode}.log"), "w")
    with psycopg.connect(PG_DSN) as c:
        seed_rows = {t: c.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in TABLES}

    idle = "120" if faults else "15"  # a Connect restart pauses the stream for tens of seconds
    sink_args = ["cdc.sink", "--until-idle", idle, "--status", SINK_STATUS] + (["--naive"] if naive else [])
    vector_args = ["cdc.vector_sync", "--until-idle", "120" if faults else "30", "--status", VECTOR_STATUS]
    sink = start(sink_args, log)
    vector = None if naive else start(vector_args, log)
    wait_until(lambda: read_json(SINK_STATUS).get("committed", 0) >= sum(seed_rows.values()), 180, "snapshot")
    print(f"snapshot in Iceberg ({sum(seed_rows.values())} rows); starting {ops} ops at {rate}/s", flush=True)

    progress, truth = {}, {}
    worker = threading.Thread(target=lambda: truth.update(workload.run(ops, rate, seed, progress=progress)))
    worker.start()

    if faults:
        # 1) kill -9 the vector sync; the new process resumes from its last committed Kafka offset
        wait_until(lambda: progress.get("done", 0) >= ops * 0.4, ops / rate * 3 + 60, "vector-sync kill point")
        vector_before = read_json(VECTOR_STATUS)
        vector_killed_at = progress["done"]
        os.kill(vector.pid, signal.SIGKILL)
        vector.wait()
        time.sleep(2)
        vector = start(vector_args, log)
        # 2) kill -9 Kafka Connect: Debezium resumes from its last flushed source offset and re-sends what followed
        wait_until(lambda: progress.get("done", 0) >= ops * 0.6, ops / rate * 3 + 60, "connect kill point")
        connect_killed_at = progress["done"]
        kill_time = utcnow()
        t0 = time.time()
        subprocess.run(["docker", "kill", "cdc-connect"], check=True, capture_output=True)
        subprocess.run(["docker", "start", "cdc-connect"], check=True, capture_output=True)
        wait_until(connector_running, 300, "connector restart")
        connect_down_s = round(time.time() - t0, 1)
        print(f"vector sync killed at op {vector_killed_at}; Kafka Connect killed at op {connect_killed_at}, "
              f"back after {connect_down_s} s", flush=True)
        worker.join()
        sink.wait()
        vector.wait()
        kill = {"vector_sync_killed_at_op": vector_killed_at,
                "vector_events_committed_before_kill": vector_before.get("events", 0),
                "connect_killed_at_op": connect_killed_at, "connect_down_s": connect_down_s}
    else:
        def in_flight():  # events the sink has read but not yet committed to Iceberg
            status = read_json(SINK_STATUS)
            return status.get("polled", 0) - status.get("committed", 0)

        # kill between two flushes, while the sink holds uncommitted events; right after a flush the kill tests nothing
        wait_until(lambda: progress.get("done", 0) >= ops * kill_at and in_flight() > 0, ops / rate * 3 + 60, "kill point")
        before = read_json(SINK_STATUS)
        killed_at_op = progress["done"]
        os.kill(sink.pid, signal.SIGKILL)
        sink.wait()
        kill_time = utcnow()
        con = changelogs()
        committed_before_kill = sum(con.execute(f"SELECT count(*) FROM {t}_changes").fetchone()[0] for t in TABLES)
        print(f"SIGKILL at op {killed_at_op}: {committed_before_kill} rows committed, "
              f"process had polled {before.get('polled')}", flush=True)
        time.sleep(2)
        sink = start(sink_args, log)
        worker.join()
        sink.wait()
        after = read_json(SINK_STATUS)
        if vector:
            vector.wait()
        kill = {"at_op": killed_at_op, "rows_committed_before_kill": committed_before_kill,
                "polled_by_killed_process": before.get("polled"),
                "in_flight_at_kill": max(0, before.get("polled", 0) - committed_before_kill),
                "committed_after_restart": after.get("committed")}

    dbt = subprocess.run([os.path.join(os.path.dirname(sys.executable), "dbt"), "build", "--profiles-dir", "."],
                         cwd=os.path.join(ROOT, "dbt"), capture_output=True, text=True,
                         env={**os.environ, "WAREHOUSE": WAREHOUSE})
    dbt_summary = re.search(r"Done\. PASS=(\d+) WARN=(\d+) ERROR=(\d+)", dbt.stdout)

    # ---- events: Iceberg changelog vs the workload's ground truth
    con = changelogs()
    per_table, missing, extra = {}, 0, 0
    for t in TABLES:
        raw, messages, events = con.execute(
            f"SELECT count(*), count(DISTINCT (_kafka_partition, _kafka_offset)), count(DISTINCT (id, _lsn)) "
            f"FROM {t}_changes").fetchone()
        got = dict(con.execute(f"SELECT _op, count(DISTINCT (id, _lsn)) FROM {t}_changes GROUP BY _op").fetchall())
        want = {"r": seed_rows[t], **{k.split(".")[1]: n for k, n in truth["events"].items() if k.startswith(t + ".")}}
        missing += sum(max(0, n - got.get(op, 0)) for op, n in want.items())
        extra += sum(max(0, n - want.get(op, 0)) for op, n in got.items())
        per_table[t] = {"expected": want, "in_iceberg": got, "rows": raw, "duplicate_rows": raw - messages,
                        "debezium_redeliveries": messages - events}
    expected_total = sum(seed_rows.values()) + ops

    # ---- state: dbt current tables vs Postgres, every column
    analytics = duckdb.connect(os.path.join(WAREHOUSE, "analytics.duckdb"), read_only=True)
    state = {}
    with psycopg.connect(PG_DSN) as pg:
        for t in TABLES:
            cur = pg.execute(f"SELECT * FROM {t}")
            cols = [d.name for d in cur.description]
            src = set(cur.fetchall())
            lake = set(analytics.execute(f"SELECT {', '.join(cols)} FROM main.{t}_current").fetchall())
            state[t] = {"postgres_rows": len(src), "lake_rows": len(lake), "mismatched_rows": len(src ^ lake)}
    evolved = "loyalty_tier" in analytics.sql("SELECT * FROM main.customers_current LIMIT 0").columns

    # ---- latency: Postgres commit -> Iceberg commit, corrected for the Docker VM clock offset
    skew = clock_skew()
    lag = f"(epoch_us(_ingested_at) - epoch_us(_source_ts)) / 1e6 + {skew}"
    rows = " UNION ALL ".join(f"SELECT _ingested_at, _source_ts FROM {t}_changes WHERE _op <> 'r'" for t in TABLES)
    p50, p95 = con.execute(f"SELECT quantile_cont({lag}, 0.5), quantile_cont({lag}, 0.95) FROM ({rows}) "
                       f"WHERE _ingested_at < '{kill_time}'").fetchone()
    worst = con.execute(f"SELECT max({lag}) FROM ({rows})").fetchone()[0]

    result = {
        "mode": mode, "run_at": utcnow().isoformat(timespec="seconds"), "workload": truth,
        "snapshot_rows": seed_rows,
        "achieved_ops_per_s": round(ops / truth["seconds"]),
        "kill": kill,
        "events": {"expected": expected_total, "missing": missing, "extra": extra,
                   "duplicate_rows": sum(v["duplicate_rows"] for v in per_table.values()),
                   "debezium_redeliveries": sum(v["debezium_redeliveries"] for v in per_table.values()),
                   "per_table": per_table},
        "current_state_vs_postgres": state,
        "schema_evolution_loyalty_tier": evolved,
        "dbt": {"pass": int(dbt_summary[1]), "warn": int(dbt_summary[2]), "error": int(dbt_summary[3])}
               if dbt_summary else {"failed": dbt.stdout[-2000:]},
        "lag_seconds": {"p50_before_kill": round(p50, 2), "p95_before_kill": round(p95, 2),
                        "max_including_restart": round(worst, 2), "clock_skew_s": round(skew, 3)},
    }
    if vector:
        result["vector_index"] = {**read_json(VECTOR_STATUS), **consistency()}

    # each fault must have hit something, otherwise the run tested nothing
    if faults:
        hit = result["events"]["debezium_redeliveries"] > 0 and kill["vector_events_committed_before_kill"] > 0
    else:
        hit = kill["in_flight_at_kill"] > 0
    ok = (missing == extra == result["events"]["duplicate_rows"] == 0
          and all(s["mismatched_rows"] == 0 for s in state.values())
          and hit
          and result["dbt"].get("error") == 0
          and (not vector or result["vector_index"]["missing"] == result["vector_index"]["stale"]
               == result["vector_index"]["orphans"] == 0))
    result["verdict"] = "PASS" if ok else "FAIL"
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    with open(os.path.join(ROOT, "results", f"experiment_{mode}.json"), "w") as f:
        json.dump(result, f, indent=2, default=str)
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ops", type=int, default=50000)
    ap.add_argument("--rate", type=float, default=1000)
    ap.add_argument("--mode", choices=["exactly-once", "naive", "faults"], default="exactly-once")
    ap.add_argument("--kill-at", type=float, default=0.5, help="fraction of the workload done when the sink is killed")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    try:
        r = main(a.ops, a.rate, a.mode, a.kill_at, a.seed)
    finally:
        for child in CHILDREN:
            if child.poll() is None:
                child.kill()
    print(json.dumps({k: r[k] for k in ("verdict", "kill", "events", "current_state_vs_postgres", "lag_seconds")
                      if k in r} | ({"vector_index": r["vector_index"]} if "vector_index" in r else {}), indent=2, default=str))
    sys.exit(0 if r["verdict"] == "PASS" else 1)
