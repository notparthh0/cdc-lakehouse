"""End-to-end over the real dbt project: hand-made changelog -> dbt build -> SCD2 history and current state."""
import os
import subprocess
import sys

import duckdb

from cdc import sink
from tests.conftest import envelope

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
T0 = 1_700_000_000_000_000  # µs


def write(cat, table, events):
    topic = f"ecommerce.public.{table}"
    tbl = sink.ensure_table(cat, topic, sink.arrow_schema(events[0]["schema"]))
    rows = [sink.to_row(e["payload"], 0, i) for i, e in enumerate(events)]
    sink.commit(tbl, rows, {0: len(rows)})


def test_scd2_history_and_current(warehouse):
    cat = sink.catalog()
    c = lambda i, city: {"id": i, "name": f"C{i}", "city": city, "updated_at": T0}  # noqa: E731
    write(cat, "customers", [
        envelope("customers", "r", c(1, "Delhi"), lsn=100, ts_us=T0),
        envelope("customers", "r", c(2, "Pune"), lsn=100, ts_us=T0),
        envelope("customers", "u", c(1, "Noida"), lsn=200, ts_us=T0 + 1_000_000),
        envelope("customers", "u", c(1, "Noida"), lsn=200, ts_us=T0 + 1_000_000),  # Debezium redelivery
        envelope("customers", "u", c(1, "Jaipur"), lsn=300, ts_us=T0 + 2_000_000),
        envelope("customers", "d", {"id": 2, "name": None, "city": None, "updated_at": None}, lsn=400, ts_us=T0 + 3_000_000),
    ])
    write(cat, "products", [envelope("products", "r", {"id": 1, "name": "Mug", "category": "Home", "description": "Mug",
                                                       "price": "799.00", "stock_qty": 5}, lsn=100)])
    write(cat, "orders", [envelope("orders", "r", {"id": 1, "status": "placed", "total_amount": "799.00"}, lsn=100)])

    dbt = os.path.join(os.path.dirname(sys.executable), "dbt")
    run = subprocess.run([dbt, "build", "--profiles-dir", "."], cwd=os.path.join(ROOT, "dbt"), capture_output=True,
                         text=True, env={**os.environ, "WAREHOUSE": str(warehouse)})
    assert run.returncode == 0, run.stdout[-3000:]

    db = duckdb.connect(str(warehouse / "analytics.duckdb"), read_only=True)
    history = db.execute("SELECT id, city, is_current, valid_to IS NULL FROM main.customers_history ORDER BY id, _lsn").fetchall()
    assert history == [
        (1, "Delhi", False, False),
        (1, "Noida", False, False),  # the redelivered duplicate collapsed into one version
        (1, "Jaipur", True, True),
        (2, "Pune", False, False),   # closed by the delete, no tombstone version
    ]
    assert db.execute("SELECT id, city FROM main.customers_current").fetchall() == [(1, "Jaipur")]
    price = db.execute("SELECT price FROM main.products_current").fetchone()[0]
    assert str(price) == "799.00"  # Debezium decimal string cast back to exact DECIMAL(10,2)
