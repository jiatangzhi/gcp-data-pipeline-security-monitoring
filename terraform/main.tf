# ──────────────────────────────────────────────────────────────────────────────
# main.tf
# GCP infrastructure for the Security Monitoring Data Pipeline.
# Resources: BigQuery dataset + tables, GCS bucket, Cloud Run service.
# ──────────────────────────────────────────────────────────────────────────────

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

# ─── BigQuery Dataset ─────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "security_monitoring" {
  dataset_id    = var.dataset_id
  friendly_name = "Security Monitoring Pipeline"
  description   = "Dataset for user activity, login events, and transaction analytics"
  location      = "US"

  labels = {
    environment = var.environment
    team        = "data-engineering"
    managed_by  = "terraform"
  }

  # Allow the pipeline service account to write to this dataset
  access {
    role          = "WRITER"
    user_by_email = var.service_account_email
  }

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
}

# ─── BigQuery Tables ─────────────────────────────────────────────────────────
# We create all tables with a simple schema. In production, you'd define
# full JSON schemas here or use schema auto-detection on load.

resource "google_bigquery_table" "pipeline_tables" {
  for_each = toset(var.bigquery_tables)

  dataset_id = google_bigquery_dataset.security_monitoring.dataset_id
  table_id   = each.value

  # Allow re-creation without errors when Terraform re-runs
  deletion_protection = false

  description = "Table ${each.value} managed by the security monitoring pipeline"

  labels = {
    environment = var.environment
    layer       = can(regex("^raw_", each.value)) ? "raw" : can(regex("^clean_", each.value)) ? "cleaned" : "aggregated"
  }

  # Partition by ingestion date for cost efficiency
  time_partitioning {
    type = "DAY"
  }
}

# ─── Google Cloud Storage Bucket ─────────────────────────────────────────────
# Used to store raw CSV files before ingestion and pipeline artifacts.

resource "google_storage_bucket" "pipeline_data" {
  name          = "${var.bucket_name}-${var.environment}"
  location      = "US"
  force_destroy = var.environment != "prod"  # Protect prod bucket from accidents

  # Automatically delete objects after 90 days
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  # Move to cheaper storage after 30 days
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  versioning {
    enabled = true
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# Create a folder structure inside the bucket
resource "google_storage_bucket_object" "data_folder" {
  name    = "data/"
  content = " "
  bucket  = google_storage_bucket.pipeline_data.name
}

resource "google_storage_bucket_object" "logs_folder" {
  name    = "logs/"
  content = " "
  bucket  = google_storage_bucket.pipeline_data.name
}

# ─── Cloud Run Service ────────────────────────────────────────────────────────
# Runs the containerised pipeline on demand (or via Cloud Scheduler trigger).

resource "google_cloud_run_v2_service" "pipeline_service" {
  name     = "security-monitoring-pipeline"
  location = var.region

  template {
    service_account = var.service_account_email

    containers {
      image = var.cloud_run_image

      resources {
        limits = {
          cpu    = "2"
          memory = "2Gi"
        }
      }

      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }

      env {
        name  = "GCP_PROJECT"
        value = var.project_id
      }

      env {
        name  = "BQ_DATASET"
        value = var.dataset_id
      }

      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.pipeline_data.name
      }
    }

    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# IAM: Allow unauthenticated invocation in dev; lock down in prod
resource "google_cloud_run_v2_service_iam_member" "pipeline_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.pipeline_service.name
  role     = "roles/run.invoker"
  member   = var.environment == "prod" ? "serviceAccount:${var.service_account_email}" : "allUsers"
}
