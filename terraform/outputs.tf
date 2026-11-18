output "gcs_bucket_name" {
  description = "GCS bucket for Iceberg warehouse"
  value       = google_storage_bucket.lakehouse.name
}

output "bigquery_dataset" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.lakehouse.dataset_id
}

output "service_account_email" {
  description = "Service account for the pipeline"
  value       = google_service_account.cdc_pipeline.email
}

output "warehouse_path" {
  description = "GCS warehouse path for PyIceberg"
  value       = "gs://${google_storage_bucket.lakehouse.name}/warehouse"
}
