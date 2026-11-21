"""Builders for Debezium envelopes, so tests run without Docker or Kafka."""
import pytest

from cdc import sink

COLUMNS = {
    "customers": [("id", "int32", None), ("name", "string", None), ("city", "string", None),
                  ("updated_at", "int64", "io.debezium.time.MicroTimestamp")],
    "products": [("id", "int32", None), ("name", "string", None), ("category", "string", None),
                 ("description", "string", None), ("price", "string", None), ("stock_qty", "int32", None)],
    "orders": [("id", "int32", None), ("status", "string", None), ("total_amount", "string", None)],
}


def envelope(table, op, row, lsn, ts_us=1_700_000_000_000_000, extra_cols=()):
    cols = [{"field": n, "type": t, **({"name": lt} if lt else {})} for n, t, lt in COLUMNS[table] + list(extra_cols)]
    schema = {"type": "struct", "fields": [
        {"field": "before", "type": "struct", "fields": cols},
        {"field": "after", "type": "struct", "fields": cols},
        {"field": "source", "type": "struct", "fields": []},
        {"field": "op", "type": "string"}]}
    payload = {"before": row if op == "d" else None, "after": None if op == "d" else row, "op": op,
               "source": {"lsn": lsn, "txId": lsn, "ts_us": ts_us}}
    return {"schema": schema, "payload": payload}


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    monkeypatch.setattr(sink, "WAREHOUSE", str(tmp_path))
    return tmp_path
