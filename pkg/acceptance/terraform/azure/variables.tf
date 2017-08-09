# Path to the cockroach binary. An empty value results in the latest official
# binary being used.
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
  default = "0"
}

# Azure configs.

variable "azure_location" {
  default = "eastus"
}

variable "azure_image" {
  default = "canonical:UbuntuServer:16.04.0-LTS:latest"
}

variable "azure_resource_group" {
  default = "cockroach-nightly"
}

variable "azure_vhd_storage_account" {
  # Only lowercase letters and numbers are allowed by Azure. This account needs
  # to belong to ${var.azure_resource_group}.
  #
  # This must be created before applying this Terraform config, because creation
  # of storage accounts through the Azure API routinely timeout as of 1/4/2017.
  # Creating storage accounts dynamically also requires dynamic generation of
  # a storage account name, which seems to trigger this issue:
  #
  # https://github.com/hashicorp/terraform/issues/6699
  #
  # Using a static storage account name also has the benefit of making it easier
  # to expunge leaked VHDs, which continue to occur as of 2017-08-09.
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
