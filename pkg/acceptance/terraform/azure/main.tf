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

# Supervisor config for CockroachDB nodes.
data "template_file" "supervisor" {
  count = "${var.num_instances}"
  template = "${file("../common/supervisor.conf.tpl")}"
  depends_on = [ "azurerm_virtual_machine.cockroach" ]

  vars {
    stores = "${var.stores}"
    cockroach_port = "${var.sql_port}"
    # The value of the --join flag must be empty for the first node,
    # and a running node for all others. We build a list of addresses
    # shifted by one (first element is empty), then take the value at index "instance.index".
    join_address = "${element(concat(split(",", ""), azurerm_public_ip.cockroach.*.fqdn), count.index == 0 ? 0 : 1)}"
    cockroach_flags = "${var.cockroach_flags}"
    # If the following changes, (*terrafarm.Farmer).Add() must change too.
    cockroach_env = "${var.cockroach_env}"
    benchmark_name = "${var.benchmark_name}"
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
    source = "../common/download_binary.sh"
    destination = "/home/ubuntu/download_binary.sh"
  }

  provisioner "file" {
    source = "../common/nodectl"
    destination = "/home/ubuntu/nodectl"
  }

  # This writes the filled-in supervisor template. It would be nice if we could
  # use rendered templates in the file provisioner.
  provisioner "remote-exec" {
    inline = <<FILE
echo '${element(data.template_file.supervisor.*.rendered, count.index)}' > supervisor.conf
FILE
  }

  provisioner "file" {
    # If no binary is specified, we'll copy /dev/null (always 0 bytes) to the
    # instance. The "remote-exec" block will then overwrite that. There's no
    # such thing as conditional file copying in Terraform, so we fake it.
    source = "${coalesce(var.cockroach_binary, "/dev/null")}"
    destination = "/home/ubuntu/cockroach"
  }

  # Launch CockroachDB.
  provisioner "remote-exec" {
    inline = [
      # For consistency with other Terraform configs, we create the store in
      # /mnt/data0.
      "sudo mkdir /mnt/data0",
      "sudo chown ubuntu:ubuntu /mnt/data0",
      # This sleep is needed to avoid apt-get errors below. It appears that when
      # the VM first launches, something is interfering with launches of apt-get.
      "sleep 30",
      # Install test dependencies. NTP synchronization is especially needed for
      # Azure VMs.
      "sudo apt-get -qqy update >/dev/null",
      "sudo apt-get -qqy install supervisor ntpdate >/dev/null",
      "sudo ntpdate -b pool.ntp.org",
      "sudo apt-get -qqy install ntp >/dev/null",
      "sudo sed -i  's/^#statsdir/statsdir/' /etc/ntp.conf",
      "sudo service supervisor stop",
      # TODO(cuongdo): Remove this dependency on Google Cloud SDK after we move
      # the test data to Azure Storage.
      "export CLOUD_SDK_REPO=\"cloud-sdk-$(lsb_release -c -s)\"",
      "echo \"deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main\" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list",
      "curl -sS https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -",
      "sudo apt-get -qqy update >/dev/null",
      "sudo apt-get -qqy install google-cloud-sdk >/dev/null",
      # Install CockroachDB.
      "mkdir /mnt/data0/logs",
      "ln -sf /mnt/data0/logs logs",
      "chmod 755 cockroach nodectl",
      "[ $(stat --format=%s cockroach) -ne 0 ] || bash download_binary.sh cockroach/cockroach ${var.cockroach_sha}",
      "if [ ! -e supervisor.pid ]; then supervisord -c supervisor.conf; fi",
      "supervisorctl -c supervisor.conf start cockroach",
      # Install load generators.
      "bash download_binary.sh examples-go/block_writer ${var.block_writer_sha}",
      "bash download_binary.sh examples-go/photos ${var.photos_sha}",
    ]
  }
}
