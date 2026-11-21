import pyarrow as pa

from cdc import sink
from tests.conftest import envelope

TOPIC = "ecommerce.public.customers"


def test_schema_maps_debezium_types():
    s = sink.arrow_schema(envelope("customers", "c", {}, 1)["schema"])
    assert s.field("id").type == pa.int32()
    assert s.field("updated_at").type == pa.timestamp("us")  # MicroTimestamp, not a raw int64
    assert s.names[:7] == [f.name for f in sink.META]


def test_delete_row_comes_from_before_image():
    env = envelope("customers", "d", {"id": 7, "name": None, "city": None, "updated_at": None}, lsn=42)
    row = sink.to_row(env["payload"], partition=1, offset=99)
    assert (row["id"], row["_op"], row["_lsn"], row["_kafka_partition"], row["_kafka_offset"]) == (7, "d", 42, 1, 99)


def test_offsets_live_in_the_iceberg_commit(warehouse):
    cat = sink.catalog()
    env = envelope("customers", "c", {"id": 1, "name": "A", "city": "Delhi", "updated_at": 1}, lsn=10)
    tbl = sink.ensure_table(cat, TOPIC, sink.arrow_schema(env["schema"]))
    assert sink.stored_offsets(cat, TOPIC) == {}  # nothing committed yet: start from the beginning
    sink.commit(tbl, [sink.to_row(env["payload"], 0, 0)], {0: 1, 2: 5})
    assert sink.stored_offsets(cat, TOPIC) == {0: 1, 2: 5}
    assert tbl.scan().to_arrow().num_rows == 1


def test_offsets_survive_a_commit_by_another_writer(warehouse):
    cat = sink.catalog()
    env = envelope("customers", "c", {"id": 1, "name": "A", "city": "Delhi", "updated_at": 1}, lsn=10)
    tbl = sink.ensure_table(cat, TOPIC, sink.arrow_schema(env["schema"]))
    sink.commit(tbl, [sink.to_row(env["payload"], 0, 0)], {0: 1})
    tbl.append(tbl.scan().to_arrow())  # another writer (e.g. a maintenance job): a snapshot without kafka.offsets
    assert len(tbl.metadata.snapshots) == 2
    assert sink.stored_offsets(cat, TOPIC) == {0: 1}  # not {} -> would replay the topic from offset 0


def test_new_source_column_evolves_the_table(warehouse):
    cat = sink.catalog()
    old = envelope("customers", "c", {"id": 1, "name": "A", "city": "Delhi", "updated_at": 1}, lsn=10)
    tbl = sink.ensure_table(cat, TOPIC, sink.arrow_schema(old["schema"]))
    sink.commit(tbl, [sink.to_row(old["payload"], 0, 0)], {0: 1})
    new = envelope("customers", "u", {"id": 1, "name": "A", "city": "Pune", "updated_at": 2, "loyalty_tier": "gold"},
                   lsn=11, extra_cols=[("loyalty_tier", "string", None)])
    tbl = sink.ensure_table(cat, TOPIC, sink.arrow_schema(new["schema"]))
    sink.commit(tbl, [sink.to_row(new["payload"], 0, 1)], {0: 2})
    rows = sorted(tbl.scan().to_arrow().to_pylist(), key=lambda r: r["_lsn"])
    assert [r["loyalty_tier"] for r in rows] == [None, "gold"]
