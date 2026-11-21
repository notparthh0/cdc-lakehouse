# CDC Lakehouse

PostgreSQL changes streamed into an Apache Iceberg lakehouse **exactly-once**, rebuilt into **SCD Type 2 history** with dbt,
and used to keep a **pgvector semantic-search index** fresh, all from one Debezium change stream.

```
PostgreSQL 16 ──WAL──▶ Debezium 3.0 ──▶ Kafka (KRaft, 3 partitions/table)
 (orders, products,      (Kafka Connect)        │
  customers)                                    ├──▶ cdc/sink.py ──▶ Iceberg changelog tables ──▶ dbt (DuckDB)
                                                │    exactly-once:      cdc.<table>_changes        stg: dedupe on (id, LSN)
                                                │    offsets live in                               history: SCD Type 2
                                                │    the Iceberg commit                            current: = Postgres
                                                │
                                                └──▶ cdc/vector_sync.py ──▶ pgvector (bge-small, local ONNX)
                                                     re-embeds only rows whose text changed
```

## Results

`make experiment` on an Apple M4 laptop (everything in local Docker), 2026-11-21. Raw output:
[`results/experiment_exactly-once.json`](results/experiment_exactly-once.json). Earlier runs of previous versions of
the code, with the same seed, also passed.

| What | Measured |
|---|---|
| Workload | 50,000 random inserts, updates and deletes across 3 tables at 1,000 ops/s, plus 190 snapshot rows |
| Fault | the sink was `kill -9`ed at op 25,449, holding 510 events it had read but not committed |
| Events in Iceberg | **50,190 of 50,190**, 0 missing, 0 duplicate rows; the restarted sink committed exactly the remaining 25,247 |
| Current state vs Postgres | **12,606 of 12,606 rows identical**, every column, after dbt |
| Schema change mid-run | `ALTER TABLE customers ADD COLUMN loyalty_tier` at 75% of the run: the Iceberg table gained the column without a restart |
| Lag, Postgres commit → Iceberg commit | p50 0.85 s, p95 1.42 s (before the kill); 4.7 s worst case, across the kill and restart |
| dbt | 18 of 18 models and tests pass |
| Vector index | 933 of 933 live products indexed with their current text (0 missing, 0 stale, 0 orphaned); p95 2.3 s from Postgres commit to searchable; 3,108 embeddings for 15,132 product change events |

**Baseline:** the v1 version of this repo used a consumer with Kafka auto-commit. The same workload and the same kill
([`results/experiment_naive.json`](results/experiment_naive.json)) gave:
- **511 duplicate rows** in Iceberg (an earlier run gave 2,550: the count depends on where the kill falls in the
  5-second auto-commit interval);
- a **46 s stall** while the consumer group waited for the dead member's session to expire.

Its final state was still correct, but only because dbt dedupes on (id, LSN). Auto-commit can also *lose* events: an
offset can be committed before its row is written. That did not happen in this run.

## How it works

**Exactly-once into Iceberg (`cdc/sink.py`).** Each Iceberg commit stores the next Kafka offset of every partition it
covers in the snapshot summary (`kafka.offsets`). Data and position therefore move in one atomic commit. On start, the
sink reads those offsets and `assign()`s its partitions there; it does not use consumer-group commits.
- A crash before a commit replays exactly the uncommitted events.
- A crash after a commit replays nothing.

This is the same idea the Iceberg Kafka Connect sink uses.

**Tables follow the source.** The Iceberg schema is derived from Debezium's own schema (MicroTimestamp → `timestamp`,
NUMERIC → exact decimal string, cast to `DECIMAL(10,2)` in dbt). A new source column is added with
`union_by_name`, after the rows buffered under the old schema are committed.

**SCD Type 2 from the changelog, not from dbt snapshots.** A dbt snapshot only sees the state at the moment it runs,
so any change between two runs is lost. Here every captured change becomes a version: order 69 went through 14 status
versions in the run.
- Versions are ordered by **WAL position (LSN)**, not commit time, which can tie.
- A delete closes the last version and adds none.
- Staging dedupes on (id, LSN), because Debezium itself is at-least-once (it can re-send events after a Connect crash).

**Vector index kept fresh by CDC (`cdc/vector_sync.py`).** It runs as its own consumer group on the products topic.
Per batch:
- it keeps the latest event per product (all events of a row share a partition);
- it deletes removed products;
- it re-embeds only products whose name, category or description hash changed, so price and stock updates never call
  the model.

Writes are idempotent upserts and deletes by id, and offsets are committed after the pgvector transaction. That makes
it at-least-once, and a replay is harmless. Embeddings come from `bge-small-en-v1.5` running locally, so there is no
API key and no rate limit. A free-tier Gemini key managed about 100 texts per minute, which made the index lag
meaningless.

**Ground truth, not self-reported counters (`cdc/experiment.py`).** The workload logs its own count of changes per
table and operation; each operation is one row in its own transaction. The checks compare Iceberg with that count, and
dbt's `*_current` tables with `SELECT *` from Postgres. Lag is corrected for the Docker VM clock offset (measured at
−0.001 s). The kill is timed for a moment when the sink holds uncommitted events. The verdict is FAIL unless the
killed process really held some (`in_flight_at_kill > 0`), so the kill can't pass by doing nothing.

## Run it

Needs Docker and Python 3.12.

```bash
uv venv --python 3.12 .venv && uv pip install -r requirements.txt   # or python -m venv + pip
make test                # 9 tests incl. a real dbt build over an Iceberg changelog; no Docker needed
make experiment          # fresh stack -> 50k changes -> kill -9 mid-run -> verification (~2 min)
make experiment-naive    # the auto-commit baseline (reports FAIL by design)
```

Piece by piece:

```bash
make reset up            # fresh Postgres, Kafka, Debezium; registers the connector (initial snapshot)
make sink                # terminal 1: Kafka -> Iceberg
make vector-sync         # terminal 2: products -> pgvector
make search Q="something to keep my drinks cold"   # on the 40 seed products, before synthetic ones dilute it
make workload OPS=5000 RATE=200
make dbt                 # staging -> history -> current, with tests
```

Point-in-time query, for example the status of every order at a given moment:

```sql
-- in warehouse/analytics.duckdb; pick any moment inside your run
SELECT id, status FROM main.orders_history
WHERE valid_from <= TIMESTAMP '2026-11-21 17:40:00' AND coalesce(valid_to, 'infinity'::TIMESTAMP) > TIMESTAMP '2026-11-21 17:40:00';
```

## Layout

| Path | What |
|---|---|
| `docker-compose.yml`, `postgres/init.sql`, `connectors/` | source DB (+ `search` DB with pgvector), Kafka in KRaft mode, Debezium connector config |
| `cdc/sink.py` | Kafka → Iceberg changelog, exactly-once; `--naive` runs the auto-commit baseline |
| `cdc/vector_sync.py` | products topic → embeddings → pgvector, plus `--search` |
| `cdc/workload.py` | random OLTP workload that records its own ground truth |
| `cdc/experiment.py` | the measured run and every check |
| `dbt/` | dedupe → SCD2 history → current state (dbt-duckdb reads Iceberg via PyIceberg) |
| `tests/` | sink, vector-sync planning, and an end-to-end dbt SCD2 test |

## Known limits

- **One sink process.** There is no coordination between two concurrent sinks: Iceberg's optimistic commit would
  reject one of them, but the sink does not handle that rejection.
- **Small files.** Each table gets one commit and one data file per second. Compaction and snapshot expiry are not
  implemented yet.
- **Full rebuilds.** dbt rebuilds the history tables on every run. That is fine at this size; incremental models are
  the next step.
- **Primary keys.** The key column is assumed to be `id`, and SCD2 `valid_from` for snapshot rows is the snapshot time.
- **Kafka Connect crash.** Not tested. Debezium is at-least-once; the (id, LSN) dedupe is meant to cover it, but this
  has not been measured. That key holds inside multi-row transactions: checked by hand, two updates of the same row in
  one transaction got distinct LSNs. A kill of the vector sync has not been measured either.
- **Embedding savings.** How many embeddings get skipped depends on the workload mix: most product changes here are
  price and stock updates. The index-lag percentiles cover every re-embedding after the snapshot.
- **Local only.** There is no cloud deployment. The lag is measured to the start of the Iceberg commit.
