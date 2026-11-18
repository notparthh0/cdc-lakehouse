# CDC + Lakehouse Pipeline

**Change Data Capture from PostgreSQL → Apache Iceberg on Cloud Storage**

```
PostgreSQL (Docker)
      │
      ▼ Debezium captures every INSERT / UPDATE / DELETE
Kafka Connect (Kafka)
      │
      ▼ CDC events (JSON) on Kafka topics
Python Iceberg Consumer (PyIceberg)
      │
      ▼ Writes Apache Iceberg tables
MinIO (local) / GCS (cloud)
      │
      ▼ Queried via
DuckDB (local) / BigQuery (cloud)
```

## Tech Stack

| Component | Technology |
|---|---|
| Source Database | PostgreSQL 15 (Docker) |
| Change Data Capture | Debezium 2.4 (Kafka Connect) |
| Message Broker | Apache Kafka (Confluent 7.5) |
| Iceberg Writer | Python + PyIceberg |
| Local Object Store | MinIO (S3-compatible) |
| Cloud Object Store | Google Cloud Storage (GCS) |
| Local Query Engine | DuckDB |
| Cloud Query Engine | BigQuery (external tables) |
| Infrastructure as Code | Terraform (GCP) |
| Monitoring | Kafka UI |

## Project Structure

```
cdc-lakehouse/
├── docker-compose.yml          # All local services
├── connect/
│   └── Dockerfile              # Kafka Connect + Debezium plugin
├── postgres/
│   └── init.sql                # E-commerce schema + seed data
├── connectors/
│   └── source-postgres.json    # Debezium connector config
├── consumer/
│   ├── iceberg_consumer.py     # Kafka → Iceberg writer
│   └── requirements.txt        # Python dependencies
├── terraform/
│   ├── main.tf                 # GCS + BigQuery + IAM
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── scripts/
│   ├── setup.sh                # One-command full setup
│   ├── register-connector.sh   # Register Debezium connector
│   ├── simulate_changes.py     # Generate live CDC events
│   └── query_iceberg.py        # Query Iceberg via DuckDB
├── .gitignore
└── README.md
```

## Quick Start (Local — No Cloud Needed)

### Prerequisites
- Docker Desktop
- Python 3.10+
- pip

### Step 1 — Start the pipeline
```bash
cd cdc-lakehouse
chmod +x scripts/*.sh
bash scripts/setup.sh
```

### Step 2 — Start the Iceberg consumer
```bash
cd consumer
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python iceberg_consumer.py
```

### Step 3 — Simulate database changes (new terminal)
```bash
pip install psycopg2-binary
python scripts/simulate_changes.py
```

Watch the consumer terminal — you'll see INSERTs, UPDATEs, and DELETEs being captured and written to Iceberg.

### Step 4 — Query the Iceberg tables
```bash
pip install duckdb
python scripts/query_iceberg.py
```

### Step 5 — Explore the UI
- **Kafka UI:** http://localhost:8080 (see topics, messages, connectors)
- **MinIO Console:** http://localhost:9001 (see Iceberg files — login: minioadmin/minioadmin)

## Deploy to GCP (Optional)

### Prerequisites
- GCP account with free trial
- `gcloud` CLI installed
- Terraform installed

### Steps
```bash
# 1. Authenticate
gcloud auth application-default login

# 2. Configure Terraform
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your project ID

# 3. Deploy
terraform init
terraform plan
terraform apply

# 4. Update consumer config to use GCS instead of MinIO
# Change CATALOG_CONFIG in iceberg_consumer.py:
#   "warehouse": "gs://YOUR_BUCKET/warehouse"
#   Remove s3.* keys, add gcs.* keys
```

## Database Schema

### E-Commerce Tables (PostgreSQL)
- **customers** — id, name, email, city, timestamps
- **products** — id, name, category, price, stock_qty, timestamps
- **orders** — id, customer_id, product_id, quantity, total_amount, status, timestamps

### CDC Metadata Columns (added by consumer)
- `__op` — c (create), u (update), d (delete), r (snapshot read)
- `__source_ts` — Debezium source timestamp
- `__ingested_at` — when the consumer processed the event
- `__deleted` — true if the row was deleted

## How CDC Works

1. PostgreSQL writes changes to its **Write-Ahead Log (WAL)**
2. Debezium reads the WAL via **logical replication** (pgoutput plugin)
3. Each row change becomes a **Kafka message** with before/after state
4. The Python consumer parses the Debezium envelope and writes **Iceberg tables**
5. DuckDB or BigQuery queries the Iceberg tables to get current state

## Author

Parth Alagh (02316401523)
B.Tech IT, 7th Semester — USICT, GGSIPU
