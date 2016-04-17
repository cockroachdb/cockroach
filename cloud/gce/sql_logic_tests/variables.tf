# Path to the sqllogictest repository.
variable "sqllogictest_repo" {
  default = "../../../../sqllogictest"
}

# The sql tests will be given the glob: 'subdir/*/*.test'
variable "sqllogictest_subdirectories" {
  default = "test/index/between,test/index/commute,test/index/delete,test/index/in,test/index/orderby,test/index/orderby_nosort"
}

# Sha of the binary to pull down. If none, the latest is fetched.
variable "sqllogictest_sha" {
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
