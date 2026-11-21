from cdc.vector_sync import content, digest, plan


def product(pid, desc="Warm jacket", price="10.00"):
    return {"id": pid, "name": f"P{pid}", "category": "Clothing", "description": desc, "price": price, "stock_qty": 1}


def event(op, row):
    return {"op": op, "before": row if op == "d" else None, "after": None if op == "d" else row,
            "source": {"ts_us": 1}}


def test_price_change_needs_no_embedding():
    indexed = {1: digest(content(product(1)))}
    to_embed, to_delete = plan([event("u", product(1, price="12.00"))], indexed)
    assert to_embed == [] and to_delete == set()


def test_text_change_insert_and_delete():
    indexed = {1: digest(content(product(1))), 2: digest(content(product(2)))}
    to_embed, to_delete = plan([event("u", product(1, desc="Warm jacket. Bestseller.")),
                                event("c", product(3)), event("d", {"id": 2})], indexed)
    assert sorted(e[0] for e in to_embed) == [1, 3]
    assert to_delete == {2}


def test_only_the_latest_event_per_product_counts():
    # renamed then deleted inside one batch: delete it, do not embed the intermediate text
    to_embed, to_delete = plan([event("u", product(1, desc="New text")), event("d", {"id": 1})], {})
    assert to_embed == [] and to_delete == {1}
