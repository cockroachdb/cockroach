provider "google" {
  region = "${var.gce_region}"
  credentials = ""
}

resource "google_compute_instance" "cockroach" {
  count = "${var.num_instances}"

  name = "${var.prefix}-cockroach-${count.index + 1}"
  machine_type = "${var.cockroach_machine_type}"
  zone = "${var.gce_zone}"
  tags = ["cockroach"]

  disk {
    image = "${var.gce_image}"
    size = "${var.cockroach_disk_size}" # GB
  }

  network_interface {
    network = "default"
    access_config {
        # Ephemeral
    }
  }

  metadata {
    sshKeys = "ubuntu:${file("~/.ssh/${var.key_name}.pub")}"
  }

  connection {
    user = "ubuntu"
    key_file = "~/.ssh/${var.key_name}"
  }

  service_account {
    scopes = ["https://www.googleapis.com/auth/compute.readonly", "https://www.googleapis.com/auth/devstorage.read_write"]
  }
}

resource "template_file" "supervisor" {
  count = "${var.num_instances}"
  template = "${file("supervisor.conf.tpl")}"
  vars {
    stores = "${var.stores}"
    cockroach_port = "${var.sql_port}"
    # The value of the --join flag must be empty for the first node,
    # and a running node for all others. We built a list of addresses
    # shifted by one (first element is empty), then take the value at index "instance.index".
    join_address = "${element(concat(split(",", ""), google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip), count.index)}"
    # We need to provide one node address for the block writer.
    node_address = "${google_compute_instance.cockroach.0.network_interface.0.access_config.0.assigned_nat_ip}"
    cockroach_flags = "${var.cockroach_flags}"
    cockroach_env = "${var.cockroach_env}"
  }
}

resource "null_resource" "cockroach-runner" {
  count = "${var.num_instances}"

  connection {
    user = "ubuntu"
    key_file = "~/.ssh/${var.key_name}"
    host = "${element(google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip, count.index)}"
  }

  provisioner "file" {
    source = "../../cloud/gce/download_binary.sh"
    destination = "/home/ubuntu/download_binary.sh"
  }

  provisioner "file" {
    source = "./nodectl"
    destination = "/home/ubuntu/nodectl"
  }

  # This writes the filled-in supervisor template. It would be nice if we could
  # use rendered templates in the file provisioner.
  provisioner "remote-exec" {
    inline = <<FILE
echo '${element(template_file.supervisor.*.rendered, count.index)}' > supervisor.conf
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
      "sudo apt-get -qqy update >/dev/null",
      "sudo apt-get -qqy install supervisor nethogs pv >/dev/null",
      "sudo service supervisor stop",
      "export CLOUD_SDK_REPO=\"cloud-sdk-$(lsb_release -c -s)\"",
      "echo \"deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main\" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list",
      "curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -",
      "sudo apt-get -qqy update >/dev/null",
      "sudo apt-get -qqy install google-cloud-sdk >/dev/null",
      "mkdir -p logs",
      "chmod 755 cockroach nodectl",
      "[ $(stat --format=%s cockroach) -ne 0 ] || bash download_binary.sh cockroach/cockroach ${var.cockroach_sha}",
      "if [ ! -e supervisor.pid ]; then supervisord -c supervisor.conf; fi",
      "supervisorctl -c supervisor.conf start cockroach",
    ]
  }
}
