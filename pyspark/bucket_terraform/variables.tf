variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "GCS bucket name"
  type        = string
}

variable "service_account_email" {
  description = "Service account used by Spark / Airflow / VM"
  type        = string
}

variable "app_service_account_id" {
  description = "ID for the dedicated application service account"
  type        = string
  default     = "serviceaccount"
}