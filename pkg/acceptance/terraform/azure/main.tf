# Terraform configuration for nightly tests running on Azure.
#
# To perform the required one-time Azure setup:
# 1. Create a resource group for the tests and set ${var.azure_location} to
#    its name.
# 2. Create a storage account and set ${var.azure_vhd_storage_account} to its
#    name.
# 3. Create a storage container for the previously created storage account and
#    set ${var.vhd_storage_container} to its name.

provider "azurerm" {
  # There are no Azure credentials here.
  #
  # So, set the ARM_SUBSCRIPTION_ID, ARM_CLIENT_ID, ARM_CLIENT_SECRET,
  # ARM_TENANT_ID environment variables to provide credentials for Azure
  # Resource Manager.
  #
  # See https://www.terraform.io/docs/providers/azurerm to understand the Azure
  # permissions needed to run Terraform against it.

  version = "~> 0.1"
}

provider "null" {
  version = "~> 0.1"
}

#
# Networking.
#

resource "azurerm_virtual_network" "cockroach" {
  name = "${var.prefix}-vn"
  address_space = ["192.168.0.0/16"]
  location = "${var.azure_location}"
  resource_group_name = "${var.azure_resource_group}"
}

# Firewall rules.
resource "azurerm_network_security_group" "cockroach" {
  name = "${var.prefix}-nsg"
  location = "${var.azure_location}"
  resource_group_name = "${var.azure_resource_group}"

  security_rule {
    name = "${var.prefix}-cockroach-ssh"
    priority = 100
    direction = "Inbound"
    access = "Allow"
    protocol = "Tcp"
    source_port_range = "*"
    destination_port_range = "22"
    source_address_prefix = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name = "${var.prefix}-cockroach-http"
    priority = 101
    direction = "Inbound"
    access = "Allow"
    protocol = "Tcp"
    source_port_range = "*"
    destination_port_range = "8080"
    source_address_prefix = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name = "${var.prefix}-cockroach-sql"
    priority = 102
    direction = "Inbound"
    access = "Allow"
    protocol = "Tcp"
    source_port_range = "*"
    destination_port_range = "26257"
    source_address_prefix = "*"
    destination_address_prefix = "*"
  }

  # Azure Network Security Groups have a low-priority default deny all rule.
  # See:
  # https://docs.microsoft.com/en-us/azure/virtual-network/virtual-networks-nsg#default-rules

  tags {
    environment = "test"
  }
}

resource "azurerm_subnet" "cockroach" {
  name = "${var.prefix}-subnet"
  resource_group_name = "${var.azure_resource_group}"
  virtual_network_name = "${azurerm_virtual_network.cockroach.name}"
  address_prefix = "192.168.1.0/24"
}

#
# CockroachDB nodes.
#

resource "azurerm_public_ip" "cockroach" {
  count = "${var.num_instances}"
  name = "${var.prefix}-ip-${count.index + 1}"
  location = "${var.azure_location}"
  resource_group_name = "${var.azure_resource_group}"
  public_ip_address_allocation = "dynamic"
  domain_name_label="${var.prefix}-cockroach-${count.index + 1}"

  tags {
    environment = "test"
  }
}

resource "azurerm_network_interface" "cockroach" {
  count = "${var.num_instances}"

  name = "${var.prefix}-cockroach-nic-${count.index + 1}"
  location = "${var.azure_location}"
  resource_group_name = "${var.azure_resource_group}"
  network_security_group_id = "${azurerm_network_security_group.cockroach.id}"

  ip_configuration {
    name = "testconfiguration1"
    subnet_id = "${azurerm_subnet.cockroach.id}"
    private_ip_address_allocation = "dynamic"
    public_ip_address_id = "${element(azurerm_public_ip.cockroach.*.id, count.index)}"
  }
}

resource "azurerm_virtual_machine" "cockroach" {
  count = "${var.num_instances}"
  name = "${var.prefix}-cockroach-${count.index + 1}"
  location = "${var.azure_location}"
  resource_group_name = "${var.azure_resource_group}"
  network_interface_ids = ["${element(azurerm_network_interface.cockroach.*.id, count.index)}"]
  vm_size = "${var.azure_vm_size}"
  delete_os_disk_on_termination = "true"

  storage_image_reference {
    publisher = "Canonical"
    offer = "UbuntuServer"
    sku = "16.04.0-LTS"
    version = "latest"
  }

  # Don't recreate this VM when the VHD URI changes, because that may have
  # unique identifiers that change every time this config is applied.
  lifecycle {
    ignore_changes  = [ "storage_os_disk" ]
  }

  storage_os_disk {
    name = "disk1"
    vhd_uri = "https://${var.azure_vhd_storage_account}.blob.core.windows.net/${var.vhd_storage_container}/${var.prefix}-cockroach-${count.index + 1}.vhd"
    create_option = "FromImage"
  }

  os_profile {
    computer_name = "${var.prefix}-cockroach-${count.index + 1}"
    admin_username = "ubuntu"
    # This password doesn't matter, because password auth is disabled below.
    admin_password = "password_auth_disabled"
  }

  os_profile_linux_config {
    disable_password_authentication = true
    ssh_keys {
      path = "/home/ubuntu/.ssh/authorized_keys"
      key_data = "${file("~/.ssh/${var.key_name}.pub")}"
    }
  }

  tags {
    environment = "test"
  }
}

# Set up CockroachDB nodes.
resource "null_resource" "cockroach-runner" {
  count = "${var.num_instances}"
  depends_on = [ "azurerm_virtual_machine.cockroach" ]

  connection {
    user = "ubuntu"
    private_key = "${file(format("~/.ssh/%s", var.key_name))}"
    host = "${element(azurerm_public_ip.cockroach.*.fqdn, count.index)}"
  }

  provisioner "file" {
    source = "../../../../build/disable-hyperv-timesync.sh"
    destination = "/home/ubuntu/disable-hyperv-timesync.sh"
  }

  provisioner "file" {
    source = "${var.cockroach_binary}"
    destination = "/home/ubuntu/cockroach"
  }

  # Launch CockroachDB.
  provisioner "remote-exec" {
    inline = [
      "chmod +x cockroach disable-hyperv-timesync.sh",
      # For consistency with other Terraform configs, we create the store in
      # /mnt/data0.
      "sudo mkdir /mnt/data0",
      "sudo chown ubuntu:ubuntu /mnt/data0",
      # Disable hypervisor clock sync, because it can cause an unrecoverable
      # amount of clock skew. This also forces an NTP sync.
      "./disable-hyperv-timesync.sh",
      # Send logs to local SSD.
      "mkdir /mnt/data0/logs",
      "ln -sf /mnt/data0/logs logs",
      # Install load generators.
      "curl -sfSL https://edge-binaries.cockroachdb.com/examples-go/block_writer.${var.block_writer_sha} -o block_writer",
      "chmod +x block_writer",
      "curl -sfSL https://edge-binaries.cockroachdb.com/examples-go/photos.${var.photos_sha} -o photos",
      "chmod +x photos",
    ]
  }
}
