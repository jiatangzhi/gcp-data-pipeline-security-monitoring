# ──────────────────────────────────────────────────────────────────────────────
# outputs.tf
# Outputs useful connection strings and resource identifiers after deployment.
# Access via: terraform output <output_name>
# ──────────────────────────────────────────────────────────────────────────────

output "bigquery_dataset_id" {
  description = "The BigQuery dataset ID for the security monitoring pipeline"
  value       = google_bigquery_dataset.security_monitoring.dataset_id
}

output "bigquery_dataset_location" {
  description = "GCP region / location of the BigQuery dataset"
  value       = google_bigquery_dataset.security_monitoring.location
}

output "bigquery_table_ids" {
  description = "List of all BigQuery table IDs created in the dataset"
  value       = [for t in google_bigquery_table.pipeline_tables : t.table_id]
}

output "gcs_bucket_name" {
  description = "Name of the GCS bucket used for raw data files"
  value       = google_storage_bucket.pipeline_data.name
}

output "gcs_bucket_url" {
  description = "GCS bucket URL (gs://...)"
  value       = "gs://${google_storage_bucket.pipeline_data.name}"
}

output "cloud_run_service_url" {
  description = "URL of the deployed Cloud Run pipeline service"
  value       = google_cloud_run_v2_service.pipeline_service.uri
}

output "cloud_run_service_name" {
  description = "Name of the Cloud Run service"
  value       = google_cloud_run_v2_service.pipeline_service.name
}

output "environment" {
  description = "Deployment environment"
  value       = var.environment
}

output "project_id" {
  description = "GCP project ID"
  value       = var.project_id
}
