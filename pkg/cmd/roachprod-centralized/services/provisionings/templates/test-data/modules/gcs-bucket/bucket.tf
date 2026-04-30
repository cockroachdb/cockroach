resource "google_storage_bucket" "bucket" {
  force_destroy               = var.force_destroy
  location                    = var.location
  name                        = var.bucket_name
  project                     = var.project_id
  public_access_prevention    = var.public_access_prevention
  storage_class               = var.storage_class
  uniform_bucket_level_access = var.uniform_bucket_level_access
  dynamic "encryption" {
    for_each = var.encryption_kms_key_name != null ? [1] : []
    content {
      default_kms_key_name = var.encryption_kms_key_name
    }
  }
}
