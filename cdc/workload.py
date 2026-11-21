"""Random OLTP workload on the source database that records its own ground truth.

Each operation is one statement touching exactly one row in its own transaction, so Debezium
must emit exactly one change event per operation. Part-way through, it runs
ALTER TABLE customers ADD COLUMN loyalty_tier, which the sink has to absorb without a restart.
"""
from __future__ import annotations

import argparse
import json
import random
import time
from collections import Counter
from decimal import Decimal

import psycopg

from cdc.config import PG_DSN

OPS = {  # operation -> weight
    "insert_order": 25, "update_order": 25, "delete_order": 8,
    "update_product_stock": 20, "update_product_text": 4, "insert_product": 4, "delete_product": 2,
    "insert_customer": 6, "update_customer": 6,
}
STATUSES = ["placed", "confirmed", "shipped", "delivered", "returned"]
CITIES = ["Delhi", "Mumbai", "Bangalore", "Pune", "Chennai", "Hyderabad", "Kolkata", "Jaipur", "Noida"]
ADJ = ["Compact", "Premium", "Eco", "Wireless", "Classic", "Smart", "Travel", "Pro"]
NOUN = {"Electronics": "earbuds", "Home": "blender", "Sports": "football", "Clothing": "hoodie",
        "Footwear": "trainers", "Books": "novel", "Grocery": "granola"}
TIERS = ["bronze", "silver", "gold"]


def pop_random(items: list, rng: random.Random):
    i = rng.randrange(len(items))
    items[i], items[-1] = items[-1], items[i]
    return items.pop()


def run(ops: int, rate: float, seed: int = 42, alter_at: float = 0.75, progress: dict | None = None) -> dict:
    rng = random.Random(seed)
    progress = progress if progress is not None else {}
    conn = psycopg.connect(PG_DSN, autocommit=True)
    q = conn.execute
    customers = [r[0] for r in q("SELECT id FROM customers")]
    prices = {pid: price for pid, price in q("SELECT id, price FROM products")}
    orders = {oid: pid for oid, pid in q("SELECT id, product_id FROM orders")}
    orders_per_product = Counter(orders.values())
    order_ids = list(orders)
    counts = Counter()  # (table, op) -> events Debezium must emit
    altered = q("SELECT count(*) FROM information_schema.columns WHERE table_name = 'customers' "
                "AND column_name = 'loyalty_tier'").fetchone()[0] == 1  # re-run on a stack that already has it
    tag = int(time.time())  # keeps inserted emails unique across runs with the same seed
    names, weights = list(OPS), list(OPS.values())
    start = time.time()

    for i in range(ops):
        if not altered and i >= ops * alter_at:
            q("ALTER TABLE customers ADD COLUMN IF NOT EXISTS loyalty_tier TEXT")
            altered = True
        op = rng.choices(names, weights)[0]
        if op == "delete_product":
            free = [p for p, n in orders_per_product.items() if n == 0 and p in prices]
            if not free:  # FK: only products without live orders can go; otherwise do a stock update
                op = "update_product_stock"
        if op in ("update_order", "delete_order") and not order_ids:
            op = "insert_order"

        if op == "insert_order":
            pid, qty = rng.choice(list(prices)), rng.randint(1, 3)
            oid = q("INSERT INTO orders (customer_id, product_id, quantity, total_amount) VALUES (%s,%s,%s,%s) "
                    "RETURNING id", (rng.choice(customers), pid, qty, prices[pid] * qty)).fetchone()[0]
            orders[oid] = pid
            orders_per_product[pid] += 1
            order_ids.append(oid)
            counts["orders", "c"] += 1
        elif op == "update_order":
            cur = q("UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s",
                    (rng.choice(STATUSES), rng.choice(order_ids)))
            counts["orders", "u"] += cur.rowcount
        elif op == "delete_order":
            oid = pop_random(order_ids, rng)
            counts["orders", "d"] += q("DELETE FROM orders WHERE id = %s", (oid,)).rowcount
            orders_per_product[orders.pop(oid)] -= 1
        elif op == "update_product_stock":  # price/stock only: the embedded text does not change
            pid = rng.choice(list(prices))
            prices[pid] = (prices[pid] * Decimal(rng.choice(["0.95", "1.00", "1.05"]))).quantize(Decimal("0.01"))
            cur = q("UPDATE products SET price = %s, stock_qty = GREATEST(stock_qty + %s, 0), updated_at = NOW() "
                    "WHERE id = %s", (prices[pid], rng.randint(-5, 5), pid))
            counts["products", "u"] += cur.rowcount
        elif op == "update_product_text":
            cur = q("UPDATE products SET description = description || %s, updated_at = NOW() WHERE id = %s",
                    (rng.choice([" Free delivery.", " Bestseller.", " New edition."]), rng.choice(list(prices))))
            counts["products", "u"] += cur.rowcount
        elif op == "insert_product":
            cat = rng.choice(list(NOUN))
            name = f"{rng.choice(ADJ)} {NOUN[cat]} {i}"
            price = Decimal(rng.randrange(299, 20000)).quantize(Decimal("0.01"))
            pid = q("INSERT INTO products (name, category, description, price, stock_qty) VALUES (%s,%s,%s,%s,%s) "
                    "RETURNING id", (name, cat, f"{name.lower()} for everyday use", price, rng.randint(0, 100))).fetchone()[0]
            prices[pid] = price
            orders_per_product[pid] = 0
            counts["products", "c"] += 1
        elif op == "delete_product":
            pid = rng.choice(free)
            counts["products", "d"] += q("DELETE FROM products WHERE id = %s", (pid,)).rowcount
            del prices[pid], orders_per_product[pid]
        elif op == "insert_customer":
            cid = q("INSERT INTO customers (name, email, city) VALUES (%s,%s,%s) RETURNING id",
                    (f"Customer w{i}", f"w{tag}-{i}@example.com", rng.choice(CITIES))).fetchone()[0]
            customers.append(cid)
            counts["customers", "c"] += 1
        elif op == "update_customer":
            sql = "UPDATE customers SET city = %s, updated_at = NOW()" + (", loyalty_tier = %s" if altered else "")
            params = [rng.choice(CITIES)] + ([rng.choice(TIERS)] if altered else []) + [rng.choice(customers)]
            counts["customers", "u"] += q(sql + " WHERE id = %s", params).rowcount

        progress["done"] = i + 1
        ahead = start + (i + 1) / rate - time.time()
        if ahead > 0:
            time.sleep(ahead)

    conn.close()
    truth = {"ops": ops, "rate": rate, "seed": seed, "seconds": round(time.time() - start, 1),
             "events": {f"{t}.{op}": n for (t, op), n in sorted(counts.items())}}
    assert sum(counts.values()) == ops, "every operation must change exactly one row"
    return truth


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ops", type=int, default=10000)
    ap.add_argument("--rate", type=float, default=500)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    print(json.dumps(run(a.ops, a.rate, a.seed), indent=2))
