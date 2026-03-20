# ──────────────────────────────────────────────────────────────────────────────
# variables.tf
# Input variables for the GCP Security Monitoring pipeline infrastructure.
# Override defaults via terraform.tfvars or -var flags.
# ──────────────────────────────────────────────────────────────────────────────

variable "project_id" {
  description = "GCP Project ID where resources will be created"
  type        = string
  default     = "my-gcp-project"
}

variable "region" {
  description = "GCP region for resource deployment"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

variable "dataset_id" {
  description = "BigQuery dataset ID for the security monitoring pipeline"
  type        = string
  default     = "security_monitoring"
}

variable "bucket_name" {
  description = "Name of the GCS bucket for raw data files"
  type        = string
  default     = "security-monitoring-data-bucket"
}

variable "service_account_email" {
  description = "Service account email used by Cloud Run / Cloud Functions"
  type        = string
  default     = "pipeline-sa@my-gcp-project.iam.gserviceaccount.com"
}

variable "cloud_run_image" {
  description = "Docker image URI for the Cloud Run pipeline service"
  type        = string
  default     = "gcr.io/my-gcp-project/security-monitoring-pipeline:latest"
}

variable "bigquery_tables" {
  description = "List of BigQuery table names to create in the dataset"
  type        = list(string)
  default = [
    "raw_events",
    "raw_logins",
    "raw_transactions",
    "clean_events",
    "clean_logins",
    "clean_transactions",
    "agg_dau",
    "agg_failed_login_rate",
    "agg_sales_per_region",
    "agg_suspicious_users",
  ]
}
