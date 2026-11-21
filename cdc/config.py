import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TABLES = ("customers", "products", "orders")
TOPICS = tuple(f"ecommerce.public.{t}" for t in TABLES)
PG_DSN = os.getenv("PG_DSN", "postgresql://cdc_user:cdc_pass@localhost:5433/ecommerce")
SEARCH_DSN = os.getenv("SEARCH_DSN", "postgresql://cdc_user:cdc_pass@localhost:5433/search")
WAREHOUSE = os.path.abspath(os.getenv("WAREHOUSE", "warehouse"))
