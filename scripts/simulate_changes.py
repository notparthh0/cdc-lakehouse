"""
Simulate live database changes for CDC testing.
Connects to PostgreSQL and runs INSERTs, UPDATEs, and DELETEs
so Debezium captures them in real time.

Author: Parth Alagh (02316401523)
"""

import psycopg2
import time
import random

POSTGRES_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "ecommerce",
    "user":     "cdc_user",
    "password": "cdc_pass",
}

def simulate():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    print("=" * 50)
    print("  CDC Change Simulator — PostgreSQL")
    print("=" * 50)

    # --- Round 1: New customers ---
    print("\n📥 Round 1 — Inserting new customers...")
    time.sleep(2)

    new_customers = [
        ("Vikas Joshi",   "vikas@gmail.com",   "Pune"),
        ("Neha Kapoor",   "neha@gmail.com",    "Chennai"),
        ("Arjun Mehta",   "arjun@gmail.com",   "Jaipur"),
    ]
    for name, email, city in new_customers:
        cur.execute(
            "INSERT INTO customers (name, email, city) VALUES (%s, %s, %s) RETURNING id",
            (name, email, city)
        )
        cid = cur.fetchone()[0]
        print(f"   ✅ Inserted customer: {name} (id={cid})")
        time.sleep(1)

    # --- Round 2: New orders ---
    print("\n📥 Round 2 — Placing new orders...")
    time.sleep(2)

    new_orders = [
        (6, 2, 1, 29999.00, "placed"),
        (7, 1, 1, 99999.00, "placed"),
        (8, 5, 3, 26997.00, "placed"),
    ]
    for cid, pid, qty, total, status in new_orders:
        cur.execute(
            "INSERT INTO orders (customer_id, product_id, quantity, total_amount, status) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (cid, pid, qty, total, status)
        )
        oid = cur.fetchone()[0]
        print(f"   ✅ Order #{oid}: customer={cid}, product={pid}, total=₹{total}")
        time.sleep(1)

    # --- Round 3: Update order statuses ---
    print("\n📝 Round 3 — Updating order statuses...")
    time.sleep(2)

    updates = [
        (1, "shipped"),
        (4, "shipped"),
        (6, "confirmed"),
    ]
    for oid, new_status in updates:
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s",
            (new_status, oid)
        )
        print(f"   📝 Order #{oid} → status = '{new_status}'")
        time.sleep(1)

    # --- Round 4: Update stock quantities ---
    print("\n📝 Round 4 — Updating product stock...")
    time.sleep(2)

    stock_updates = [
        (1, -1,  "MacBook Air M3"),
        (2, -3,  "Sony WH-1000XM5"),
        (5, -5,  "Nike Air Max"),
    ]
    for pid, change, pname in stock_updates:
        cur.execute(
            "UPDATE products SET stock_qty = stock_qty + %s, updated_at = NOW() WHERE id = %s",
            (change, pid)
        )
        print(f"   📝 {pname}: stock {change:+d}")
        time.sleep(1)

    # --- Round 5: Update customer info ---
    print("\n📝 Round 5 — Customer address changes...")
    time.sleep(2)

    cur.execute("UPDATE customers SET city = 'Noida', updated_at = NOW() WHERE id = 1")
    print("   📝 Amit Sharma moved: Delhi → Noida")
    time.sleep(1)

    cur.execute("UPDATE customers SET email = 'priya.verma@outlook.com', updated_at = NOW() WHERE id = 2")
    print("   📝 Priya Verma changed email")
    time.sleep(1)

    # --- Round 6: Delete an order (cancellation) ---
    print("\n🗑️  Round 6 — Order cancellation...")
    time.sleep(2)

    cur.execute("DELETE FROM orders WHERE id = 4")
    print("   🗑️  Order #4 cancelled and deleted")
    time.sleep(1)

    # --- Round 7: Fulfil remaining orders ---
    print("\n📝 Round 7 — Fulfilling orders...")
    time.sleep(2)

    for oid in [2, 5, 6]:
        cur.execute(
            "UPDATE orders SET status = 'delivered', updated_at = NOW() WHERE id = %s",
            (oid,)
        )
        print(f"   📝 Order #{oid} → delivered")
        time.sleep(1)

    cur.close()
    conn.close()

    print("\n" + "=" * 50)
    print("  ✅ Simulation complete!")
    print("  Total changes: 3 customer inserts + 3 order inserts")
    print("                 + 6 updates + 1 delete + 3 fulfillments")
    print("=" * 50)


if __name__ == "__main__":
    simulate()
