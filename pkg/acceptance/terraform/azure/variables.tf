# Number of CockroachDB instances. This is overridden by terrafarm.
variable "num_instances" {
  default = "0"
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

# SHA of the cockroach binary to pull down. If default, the latest is fetched.
variable "cockroach_sha" {
  default = "LATEST"
}

# SHA of the block_writer binary to pull down. If default, the latest is fetched.
variable "block_writer_sha" {
  default = "LATEST"
}

# SHA of the photos binary to pull down. If default, the latest is fetched.
variable "photos_sha" {
  default = "LATEST"
}

# Prefix to prepend to all GC resource names.
variable "prefix" {
  default = "alloctest"
}
