# Port used for the load balancer and backends.
variable "sql_port" {
  default = "26257"
}

variable "http_port" {
  default = "8080"
}

# GCE region to use.
variable "gce_region" {
  default = "us-east1"
}

# GCE zone to use.
variable "gce_zone" {
  default = "us-east1-b"
}

# The GCE project under which you want to run your cluster. You'll want to
# change this.
variable "gce_project" { }

# Your JSON-format Google Cloud application credentials. You'll want to change this.
# To learn how to download your credentials, go here:
#
# https://developers.google.com/identity/protocols/application-default-credentials#howtheywork
variable "gce_account_file" { }

# GCE image name.
variable "gce_image" {
  default = "ubuntu-os-cloud/ubuntu-1510-wily-v20151021"
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

# Number of instances to start.
variable "num_instances" {
}

# SHA of the cockroach binary to pull down. If none, the latest is fetched.
variable "cockroach_sha" {
  default = ""
}

