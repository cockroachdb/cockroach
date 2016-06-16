# Number of CockroachDB instances. This is overridden by clusterctl.
variable "num_instances" {
  default = "0"
}

# Port used for the load balancer and backends.
variable "sql_port" {
  default = "26257"
}

variable "http_port" {
  default = "8080"
}

# List of stores each with its own --store flag. (--store flags from the cockroach binary).
variable "stores" {
  default = "--store=data"
}

# GCE region to use.
variable "gce_region" {
  default = "us-east1"
}

# GCE zone to use.
variable "gce_zone" {
  default = "us-east1-c"
}

# GCE image name.
variable "gce_image" {
  default = "ubuntu-os-cloud/ubuntu-1510-wily-v20151021"
}

# Machine type for non-DB nodes (e.g. load generators).
variable "gce_machine_type" {
  default = "n1-standard-4"
}

# Path to the cockroach binary. An empty value results in the latest official
# binary being used.
variable "cockroach_binary" {
  default = ""
}

# Name of the ssh key pair to use for GCE instances.
# The public key will be passed at instance creation, and the private
# key will be used by the local ssh client.
#
# The path is expanded to: ~/.ssh/<key_name>.pub
#
# If you use `gcloud compute ssh` or `gcloud compute copy-files`, you may want
# to leave this as "google_compute_engine" for convenience.
variable "key_name" {
  default = "google_compute_engine"
}

# SHA of the cockroach binary to pull down. If none, the latest is fetched.
variable "cockroach_sha" {
  default = ""
}

# SHA of the block_writer binary to pull down. If none, the latest is fetched.
variable "block_writer_sha" {
  default = ""
}

# SHA of the photos binary to pull down. If none, the latest is fetched.
variable "photos_sha" {
  default = ""
}

# Prefix to prepend to all GC resource names.
variable "prefix" {
  default = "alloctest"
}

# Additional command-line flags to pass into `cockroach start`.
variable "cockroach_flags" {
  default  = ""
}

# Machine type for CockroachDB nodes.
variable "cockroach_machine_type" {
  default = "n1-standard-4"
}

# Size of disk for CockroachDB nodes.
variable "cockroach_disk_size" {
  default = "50" # GB
}
