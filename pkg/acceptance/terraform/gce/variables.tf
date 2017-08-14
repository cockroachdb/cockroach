# Path to the cockroach binary. Must always be specified; a default is
# provided to work around https://github.com/hashicorp/terraform/issues/5425.
variable "cockroach_binary" {
  default = ""
}

# Name of the ssh key pair to use.
#
# The path is expanded to: ~/.ssh/<key_name>.pub
#
# Note that this key *must not* be password-protected. Terraform doesn't
# support password-protected keys.
variable "key_name" {
  default = "google_compute_engine"
}

# SHA of the block_writer binary to pull down. If default, the latest is fetched.
variable "block_writer_sha" {
  default = "LATEST"
}

# SHA of the photos binary to pull down. If default, the latest is fetched.
variable "photos_sha" {
  default = "LATEST"
}

# Prefix to prepend to all resource names.
variable "prefix" {
  default = "alloctest"
}

variable "num_instances" {
  # Nonsense, but terraform needs values for all variables when destroying.
  # https://github.com/hashicorp/terraform/issues/5425.
  default = "0"
}

# GCE configs.

variable "gce_region" {
  default = "us-east1"
}

variable "gce_zone" {
  default = "us-east1-c"
}

variable "gce_image" {
  default = "ubuntu-os-cloud/ubuntu-1604-lts"
}

variable "gce_machine_type" {
  default = "n1-standard-4"
}

variable "cockroach_machine_type" {
  default = "n1-standard-4"
}

variable "cockroach_root_disk_size" {
  default = "10" # GB
}

variable "cockroach_root_disk_type" {
  default = "pd-standard"
}
