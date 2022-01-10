provider "google" {
  project = var.project
  region  = var.region
}

resource "google_service_account" "metadata_publisher" {
  account_id = "metadata-publisher"
}

resource "google_storage_bucket" "release_automation_bucket" {
  name                        = var.bucket
  location = "US"
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
}

resource "google_storage_bucket_iam_binding" "metadata_publisher_writer" {
  bucket  = google_storage_bucket.release_automation_bucket.name
  role    = "roles/storage.legacyBucketWriter"
  members = ["serviceAccount:${google_service_account.metadata_publisher.email}"]
  condition {
    title       = "Limited writer"
    description = "Limited writer"
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket
    .release_automation_bucket
    .name}/objects/${var.release_qualification_prefix}')"
  }
}

# gsutil requires read access to the bucket
resource "google_storage_bucket_iam_binding" "metadata_publisher_reader" {
  bucket  = google_storage_bucket.release_automation_bucket.name
  role    = "roles/storage.objectViewer"
  members = ["serviceAccount:${google_service_account.metadata_publisher.email}"]
}
