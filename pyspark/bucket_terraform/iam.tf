resource "google_storage_bucket_iam_member" "spark_admin" {
  bucket = google_storage_bucket.surge_bucket.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${var.service_account_email}"
}

resource "google_service_account" "app_sa" {
  account_id   = var.app_service_account_id
  display_name = "Surge Pricing App Service Account"
}

resource "google_storage_bucket_iam_member" "app_sa_storage_admin" {
  bucket = google_storage_bucket.surge_bucket.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.app_sa.email}"
}

resource "google_service_account_iam_member" "compute_impersonate_app" {
  service_account_id = google_service_account.app_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${var.service_account_email}"
}