# Run the sql logic test suite on GCE.
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
#
# The used logic tests are tarred and gzipped before launching the instance.
# Test are sharded by subdirectory (see variables.tf for details), with one
# instance handling each subdirectory.
# The latest sql.test binary is fetched from S3.

provider "google" {
  region = "${var.gce_region}"
}

output "instance" {
  value = "${join(",", google_compute_instance.sql_logic_test.*.network_interface.0.access_config.0.assigned_nat_ip)}"
}

resource "google_compute_instance" "sql_logic_test" {
  count = "${length(split(",", var.sqllogictest_subdirectories))}"
  depends_on = ["null_resource.sql_tarball"]

  name = "sql-logic-test-${count.index}"
  machine_type = "${var.gce_machine_type}"
  zone = "${var.gce_zone}"
  tags = ["sqllogictest"]

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
    source = "tarball${count.index}.tgz"
    destination = "/home/ubuntu/sqltests.tgz"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo apt-get -y update",
      "sudo apt-get -y install supervisor",
      "sudo service supervisor stop",
      "bash download_binary.sh cockroach/sql.test ${var.sqllogictest_sha}",
      "mkdir -p logs",
      "tar xfz sqltests.tgz",
      "if [ ! -e supervisor.pid ]; then supervisord -c supervisor.conf; fi",
      "supervisorctl -c supervisor.conf start sql.test",
    ]
  }
}

resource "google_compute_firewall" "default" {
  name = "cockroach-sql-logic-test-firewall"
  network = "default"

  allow {
    protocol = "tcp"
    ports = ["9001"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags = ["sqllogictest"]
}

resource "null_resource" "sql_tarball" {
  count = "${length(split(",", var.sqllogictest_subdirectories))}"
  provisioner "local-exec" {
    command = "tar cfz tarball${count.index}.tgz -C ${var.sqllogictest_repo} ${element(split(",", var.sqllogictest_subdirectories),count.index)}"
  }
}
