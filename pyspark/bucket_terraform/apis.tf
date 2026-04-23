resource "google_project_service" "apis" {
  for_each = toset([
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com"
  ])

  service                    = each.key
  disable_dependent_services = false
  disable_on_destroy         = false
}
