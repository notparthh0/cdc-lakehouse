"""
Query Iceberg tables from the local warehouse using DuckDB.
Verifies that CDC events were written correctly.

Author: Parth Alagh (02316401523)
"""

import duckdb

def query_iceberg():
    print("=" * 60)
    print("  CDC Lakehouse — Iceberg Table Query (DuckDB)")
    print("=" * 60)

    con = duckdb.connect()

    # Install and load Iceberg + httpfs extensions
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Configure MinIO connection
    con.execute("""
        SET s3_region = 'us-east-1';
        SET s3_endpoint = 'localhost:9000';
        SET s3_access_key_id = 'minioadmin';
        SET s3_secret_access_key = 'minioadmin';
        SET s3_use_ssl = false;
        SET s3_url_style = 'path';
    """)

    tables = ["customers", "products", "orders"]

    for table in tables:
        print(f"\n{'─' * 60}")
        print(f"📋 Table: cdc.{table}")
        print(f"{'─' * 60}")

        try:
            # Read from Iceberg metadata
            metadata_path = f"s3://lakehouse-bucket/warehouse/cdc/{table}/metadata/v1.metadata.json"

            result = con.execute(f"""
                SELECT * FROM iceberg_scan('{metadata_path}')
                ORDER BY __source_ts DESC
                LIMIT 20
            """).fetchdf()

            print(result.to_string(index=False))
            print(f"\n   Total rows: {len(result)}")

        except Exception as e:
            print(f"   ⚠️  Could not read table: {e}")

    # --- Summary Queries ---
    print(f"\n{'=' * 60}")
    print(f"  Summary Queries")
    print(f"{'=' * 60}")

    try:
        # Latest state of orders (most recent event per ID)
        print("\n📊 Current Order Status (latest event per order):")
        result = con.execute("""
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY __source_ts DESC) as rn
                FROM iceberg_scan('s3://lakehouse-bucket/warehouse/cdc/orders/metadata/v1.metadata.json')
            )
            SELECT id, customer_id, product_id, quantity, total_amount, status, __op, __deleted
            FROM ranked
            WHERE rn = 1
            ORDER BY id
        """).fetchdf()
        print(result.to_string(index=False))

    except Exception as e:
        print(f"   ⚠️  {e}")

    try:
        # CDC event counts by operation type
        print("\n📊 CDC Events by Operation Type:")
        for table in tables:
            metadata = f"s3://lakehouse-bucket/warehouse/cdc/{table}/metadata/v1.metadata.json"
            result = con.execute(f"""
                SELECT __op, COUNT(*) as count
                FROM iceberg_scan('{metadata}')
                GROUP BY __op
                ORDER BY __op
            """).fetchdf()
            print(f"\n   {table}:")
            print(f"   {result.to_string(index=False)}")

    except Exception as e:
        print(f"   ⚠️  {e}")

    con.close()
    print(f"\n✅ Query complete.")


if __name__ == "__main__":
    query_iceberg()
