# ============================================================
# Terraform — GCP Infrastructure for CDC Lakehouse
# Provisions: GCS bucket, BigQuery dataset, BigQuery tables,
#             Service Account + IAM bindings
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ============================================================
# Service Account for CDC Pipeline
# ============================================================

resource "google_service_account" "cdc_pipeline" {
  account_id   = "cdc-lakehouse-sa"
  display_name = "CDC Lakehouse Pipeline Service Account"
  description  = "Used by the Iceberg consumer to write to GCS and manage BigQuery tables"
}

# ============================================================
# GCS Bucket — Iceberg Warehouse
# ============================================================

resource "google_storage_bucket" "lakehouse" {
  name          = "${var.project_id}-cdc-lakehouse"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    project     = "cdc-lakehouse"
    environment = "dev"
    managed_by  = "terraform"
  }
}

# Grant SA write access to the bucket
resource "google_storage_bucket_iam_member" "sa_gcs_admin" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.cdc_pipeline.email}"
}

# ============================================================
# BigQuery Dataset
# ============================================================

resource "google_bigquery_dataset" "lakehouse" {
  dataset_id    = "cdc_lakehouse"
  friendly_name = "CDC Lakehouse"
  description   = "Iceberg tables from PostgreSQL CDC pipeline"
  location      = var.region

  labels = {
    project     = "cdc-lakehouse"
    managed_by  = "terraform"
  }

  delete_contents_on_destroy = true
}

# Grant SA BigQuery access
resource "google_project_iam_member" "sa_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.cdc_pipeline.email}"
}

resource "google_project_iam_member" "sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.cdc_pipeline.email}"
}

# ============================================================
# BigQuery External Tables (read Parquet files from GCS)
# These point to the Iceberg table data in GCS
# ============================================================

resource "google_bigquery_table" "customers" {
  dataset_id          = google_bigquery_dataset.lakehouse.dataset_id
  table_id            = "customers_raw"
  description         = "CDC events from PostgreSQL customers table"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.lakehouse.name}/warehouse/cdc/customers/data/*.parquet"]
  }

  labels = {
    source     = "postgresql"
    table      = "customers"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "products" {
  dataset_id          = google_bigquery_dataset.lakehouse.dataset_id
  table_id            = "products_raw"
  description         = "CDC events from PostgreSQL products table"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.lakehouse.name}/warehouse/cdc/products/data/*.parquet"]
  }

  labels = {
    source     = "postgresql"
    table      = "products"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "orders" {
  dataset_id          = google_bigquery_dataset.lakehouse.dataset_id
  table_id            = "orders_raw"
  description         = "CDC events from PostgreSQL orders table"
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.lakehouse.name}/warehouse/cdc/orders/data/*.parquet"]
  }

  labels = {
    source     = "postgresql"
    table      = "orders"
    managed_by = "terraform"
  }
}

# ============================================================
# BigQuery Views — Materialised Current State
# Shows the latest version of each row (deduped by ID)
# ============================================================

resource "google_bigquery_table" "orders_current" {
  dataset_id          = google_bigquery_dataset.lakehouse.dataset_id
  table_id            = "orders_current"
  description         = "Current state of orders (latest CDC event per ID, excluding deletes)"
  deletion_protection = false

  view {
    query          = <<-SQL
      WITH ranked AS (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY id ORDER BY __source_ts DESC) AS rn
        FROM `${var.project_id}.${google_bigquery_dataset.lakehouse.dataset_id}.orders_raw`
      )
      SELECT * EXCEPT(rn, __op, __source_ts, __ingested_at, __deleted)
      FROM ranked
      WHERE rn = 1 AND (__deleted IS NULL OR __deleted = FALSE)
    SQL
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "customers_current" {
  dataset_id          = google_bigquery_dataset.lakehouse.dataset_id
  table_id            = "customers_current"
  description         = "Current state of customers (latest CDC event per ID)"
  deletion_protection = false

  view {
    query          = <<-SQL
      WITH ranked AS (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY id ORDER BY __source_ts DESC) AS rn
        FROM `${var.project_id}.${google_bigquery_dataset.lakehouse.dataset_id}.customers_raw`
      )
      SELECT * EXCEPT(rn, __op, __source_ts, __ingested_at, __deleted)
      FROM ranked
      WHERE rn = 1 AND (__deleted IS NULL OR __deleted = FALSE)
    SQL
    use_legacy_sql = false
  }
}
