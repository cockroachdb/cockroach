# Create the GCS bucket
module "bucket" {
  source = "./modules/gcs-bucket"

  project_id                  = var.gcp_project
  bucket_name                 = "crl-gcs-${replace(var.owner, "/[@\\-\\.]/", "_")}"
  location                    = upper(var.bucket_config.region)
  storage_class               = var.bucket_config.storage_class
  uniform_bucket_level_access = var.bucket_config.uniform_bucket_level_access
  public_access_prevention    = "inherited"
  force_destroy               = var.bucket_config.force_destroy
  labels = merge(
    var.bucket_config.labels,
    {
      type        = "gcs-bucket"
      prov_type   = "gcs-bucket"
      identifier  = var.identifier
      environment = var.environment
  })
  permissions = {
    "roles/storage.objectViewer" = ["user:${var.owner}"],
    "roles/storage.objectAdmin"  = ["user:${var.owner}"]
  }
}
