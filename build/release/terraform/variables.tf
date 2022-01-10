variable "project" {
  description = "The GCP project to use"
  type        = string
  default     = "dev-inf-devenv"
}

variable "region" {
  description = "The GCP region to create and test resources in"
  type        = string
  default     = "us-east4"
}

variable "bucket" {
  description = "Bucket name to store function source files and users.json"
  type        = string
  default     = "release-automation-dev"
}

variable "release_qualification_prefix" {
  description = "Object prefix used by the release qualification publisher"
  type        = string
  default     = "release-qualification"
}
