terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
  required_version = ">= 0.13"

}

provider "google" {
  project = var.gcp_project
}
