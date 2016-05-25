# Run the benchmarks on GCE.
# Prerequisites:
# - GCE account credentials file as specified in cockroach/cloud/gce/README.md
#
# Run with:
# # google compute SSH key in ~/.ssh/google_compute_engine{,.pub}
# $ export GOOGLE_CREDENTIALS="contents of json credentials file"
# $ export GOOGLE_PROJECT="my-google-project"
# $ terraform apply
#
# Tear down GCE resources using:
# $ terraform destroy

provider "google" {
  region = "${var.gce_region}"
}

output "instance" {
  value = "${join(",", google_compute_instance.benchmark.*.network_interface.0.access_config.0.assigned_nat_ip)}"
}

resource "google_compute_instance" "benchmark" {
  count = 1

  name = "benchmarks-${var.benchmarks_package}-${count.index}"
  name = "benchmark-${replace(var.benchmarks_package,".","-")}-${count.index}"
  machine_type = "${var.gce_machine_type}"
  zone = "${var.gce_zone}"
  tags = ["benchmark"]

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

  provisioner "file" {
    source = "supervisor.conf"
    destination = "/home/ubuntu/supervisor.conf"
  }

  provisioner "file" {
    source = "../download_binary.sh"
    destination = "/home/ubuntu/download_binary.sh"
  }

  provisioner "file" {
    source = "benchmarks.sh"
    destination = "/home/ubuntu/benchmarks.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo apt-get -y update",
      "sudo apt-get -y install supervisor",
      "sudo service supervisor stop",
      "chmod 755 benchmarks.sh",
      "bash download_binary.sh cockroach/${var.benchmarks_package}.tar.gz ${var.benchmarks_sha}",
      "tar xfz ${var.benchmarks_package}.tar.gz",
      "mkdir -p logs",
      "if [ ! -e supervisor.pid ]; then supervisord -c supervisor.conf; fi",
      "supervisorctl -c supervisor.conf start benchmarks",
    ]
  }
}

resource "google_compute_firewall" "default" {
  name = "cockroach-benchmark-${replace(var.benchmarks_package,".","-")}-firewall"
  network = "default"

  allow {
    protocol = "tcp"
    ports = ["9001"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags = ["benchmark"]
}
