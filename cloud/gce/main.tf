provider "google" {
  region = "${var.gce_region}"
  project = "${var.gce_project}"
  credentials = "${file(var.gce_account_file)}"
}

resource "google_compute_instance" "cockroach" {
  count = "${var.num_instances}"

  name = "cockroach-${count.index}"
  machine_type = "n1-standard-1"
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
    key_file = "~/.ssh/${var.key_name}"
  }

  service_account {
    scopes = ["https://www.googleapis.com/auth/compute.readonly"]
  }
}

# Generate contents of config.sh from its template file.
#
# TODO(cdo): support secure clusters
resource "template_file" "config" {
  count = "${var.num_instances}"

  template = "${file("config.sh.tpl")}"
  vars {
    sql_port = "${var.sql_port}"
    http_port = "${var.http_port}"
    local_address = "${element(google_compute_instance.cockroach.*.network_interface.0.address, count.index)}"
    # The value of the --join flag must be empty for the first node,
    # and a running node for all others. We built a list of addresses
    # shifted by one (first element is empty), then take the value at index "instance.index".
    join_address = "${element(concat(split(",", ""), google_compute_instance.cockroach.*.network_interface.0.address), count.index)}"
  }
}

resource "null_resource" "cockroach-runner" {
  count = "${var.num_instances}"

  connection {
    user = "ubuntu"
    key_file = "~/.ssh/${var.key_name}"
    host = "${element(google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip, count.index)}"
  }

  triggers {
    instance_ids = "${element(google_compute_instance.cockroach.*.id, count.index)}"
  }

  provisioner "file" {
    source = "download_binary.sh"
    destination = "/home/ubuntu/download_binary.sh"
  }

  provisioner "file" {
    source = "launch.sh"
    destination = "/home/ubuntu/launch.sh"
  }

  provisioner "file" {
    # If no binary is specified, we'll copy /dev/null (always 0 bytes) to the
    # instance. The "remote-exec" block will then overwrite that. There's no
    # such thing as conditional file copying in Terraform, so we fake it.
    source = "${coalesce(var.cockroach_binary, "/dev/null")}"
    destination = "/home/ubuntu/cockroach"
  }

  # Create config.sh, which contains all configuration parameters.
  provisioner "remote-exec" {
    inline = <<FILE
echo '${element(template_file.config.*.rendered, count.index)}' > config.sh
FILE
  }

  # Launch CockroachDB.
  provisioner "remote-exec" {
    inline = [
      "chmod 755 launch.sh config.sh",
      # If the cockroach binary is empty (i.e. no cockroach_binary was
      # specified), download the binary.
      "[ $(stat --format=%s cockroach) -ne 0 ] || bash download_binary.sh cockroach/cockroach ${var.cockroach_sha}",
      "./launch.sh",
    ]
  }
}
