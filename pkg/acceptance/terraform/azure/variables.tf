# Number of CockroachDB instances. This is overridden by terrafarm.
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

# Azure location to use.
variable "azure_location" {
  default = "eastus"
}

# GCE image name.
variable "azure_image" {
  default = "canonical:UbuntuServer:16.04.0-LTS:latest"
}

variable "azure_resource_group" {
  default = "cockroach-nightly"
}

variable "azure_vhd_storage_account" {
  # Only lowercase letters and numbers are allowed by Azure. This account needs
  # to belong to ${var.vhd_azure_resource_group}.
  #
  # This must be created before applying this Terraform config, because creation
  # of storage accounts through the Azure API routinely timeout as of 1/4/2017.
  # Creating storage accounts dynamically also requires dynamic generation of
  # a storage account name, which seems to trigger this issue:
  #
  # https://github.com/hashicorp/terraform/issues/6699
  default = "cockroachnightlyvhd"
}

variable "vhd_storage_container" {
  # This must be a storage container associated with ${var.azure_stoarge_account}.
  default = "vhds"
}

# Machine type for nodes.
variable "azure_vm_size" {
  # 4cpu, 14Gib ram, 200GiB SSD
  default = "Standard_D3"
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
# Note that this key *must not* be password-protected. Terraform doesn't support
# password-protected keys.
#
# The path is expanded to: ~/.ssh/<key_name>.pub
variable "key_name" {
  default = "azure"
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

# Environment variables to pass into CockroachDB through the supervisor config.
# This must be of the following form:
#
#   VAR1=value1[,VAR2=value2,...]
#
# Relevant supervisor docs:
#
#   http://supervisord.org/subprocess.html#subprocess-environment
#
# If this changes, (*terrafarm.Farmer).Add() must change too.
variable "cockroach_env" {
  default = ""
}

# This is included in the benchmark results at the end of our load generators
# to associate the results (for benchviz) with a specific test.
variable "benchmark_name" {
  default = "BenchmarkBlockWriter"
}
