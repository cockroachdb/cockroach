# Run the stress tests on GCE.
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
  value = "${join(",", google_compute_instance.stress.*.network_interface.0.access_config.0.assigned_nat_ip)}"
}

resource "google_compute_instance" "stress" {
  count = "${var.shard_count}"

  name = "stress-test-${count.index}"
  machine_type = "${var.gce_machine_type}"
  zone = "${var.gce_zone}"
  tags = ["stress"]

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
    source = "stress.sh"
    destination = "/home/ubuntu/stress.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo apt-get -y update",
      "sudo apt-get -y install supervisor",
      "sudo service supervisor stop",
      "chmod 755 stress.sh",
      "bash download_binary.sh cockroach/static-tests.tar.gz ${var.tests_sha}",
      "bash download_binary.sh stress/stress ${var.stress_sha}",
      "tar xfz static-tests.tar.gz",
      "mkdir -p logs",
      "export SHARD_COUNT=${var.shard_count}",
      "export SHARD_INDEX=${count.index + 1}",
      "if [ ! -e supervisor.pid ]; then supervisord -c supervisor.conf; fi",
      "supervisorctl -c supervisor.conf start stress",
    ]
  }
}

resource "google_compute_firewall" "default" {
  name = "cockroach-stress-firewall"
  network = "default"

  allow {
    protocol = "tcp"
    ports = ["9001"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags = ["stress"]
}
