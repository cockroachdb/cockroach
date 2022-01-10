terraform {
  backend "gcs" {
    bucket  = "release-automation-prod"
    prefix  = "terraform/state"
  }
}
