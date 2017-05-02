provider "google" {
  region = "${var.gce_region}"
}

resource "google_compute_instance" "cockroach" {
  count = "${var.num_instances}"

  name = "cockroach-${count.index}"
  machine_type = "${var.gce_machine_type}"
  zone = "${var.gce_zone}"
  tags = ["cockroach"]

  disk {
    image = "${var.gce_image}"
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
    private_key = "${file(format("~/.ssh/%s", var.key_name))}"
  }

  service_account {
    scopes = ["https://www.googleapis.com/auth/compute.readonly"]
  }
}

data "template_file" "supervisor" {
  count = "${var.num_instances}"
  template = "${file("supervisor.conf.tpl")}"
  vars {
    stores = "${var.stores}"
    cockroach_port = "${var.sql_port}"
    # The value of the --join flag must be empty for the first node,
    # and a running node for all others. We built a list of addresses
    # shifted by one (first element is empty), then take the value at index "instance.index".
    join_address = "${element(concat(split(",", ""), google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip), count.index == 0 ? 0 : 1)}"
    # We need to provide one node address for the block writer.
    node_address = "${google_compute_instance.cockroach.0.network_interface.0.access_config.0.assigned_nat_ip}"
  }
}

resource "null_resource" "cockroach-runner" {
  count = "${var.num_instances}"

  connection {
    user = "ubuntu"
    private_key = "${file(format("~/.ssh/%s", var.key_name))}"
    host = "${element(google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip, count.index)}"
  }

  provisioner "file" {
    source = "download_binary.sh"
    destination = "/home/ubuntu/download_binary.sh"
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
      "sudo apt-get -y update",
      "sudo apt-get -y install supervisor",
      "sudo service supervisor stop",
      "mkdir -p logs",
      "chmod 755 cockroach",
      "[ $(stat --format=%s cockroach) -ne 0 ] || bash download_binary.sh cockroach/cockroach ${var.cockroach_sha}",
      "if [ ! -e supervisor.pid ]; then supervisord -c supervisor.conf; fi",
      "supervisorctl -c supervisor.conf start cockroach",
    ]
  }
}
