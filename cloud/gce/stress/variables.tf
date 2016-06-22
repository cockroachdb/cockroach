# Sha of the test tarball to pull down. If none, the latest is fetched.
variable "tests_sha" {
  default = ""
}

variable "shard_count" {
  default="5"
}

# Sha of the stress binary to pull down. If none, the latest is fetched.
variable "stress_sha" {
  default = ""
}

# Port used by supervisord.
variable "supervisor_port" {
  default = "9001"
}

# GCE region to use.
variable "gce_region" {
  default = "us-east-1"
}

# GCE zone to use.
variable "gce_zone" {
  default = "us-east1-b"
}

# GCE machine type.
variable "gce_machine_type" {
  default = "n1-highcpu-16"
}

# GCE image name.
variable "gce_image" {
  default = "ubuntu-os-cloud/ubuntu-1510-wily-v20151021"
}

variable "key_name" {
  default = "google_compute_engine"
}
