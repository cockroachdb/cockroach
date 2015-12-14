provider "google" {
  region = "${var.gce_region}"
  project = "${var.gce_project}"
  account_file = "${file(var.gce_account_file)}"
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

  provisioner "file" {
    source = "${var.cockroach_binary}"
    destination = "/home/ubuntu/cockroach"
  }

  provisioner "file" {
    source = "launch.sh"
    destination = "/home/ubuntu/launch.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "echo 'PORT=${var.cockroach_port}' > config.sh",
      "echo 'GOSSIP=${var.gossip}' >> config.sh",
      "echo 'LB_ADDRESS=${var.load_balancer_address}' >> config.sh",
      "echo 'LOCAL_ADDRESS=${self.network_interface.0.address}' >> config.sh",
      "chmod 755 launch.sh config.sh",
      "./launch.sh ${var.action}",
      "sleep 1",
    ]
  }

  service_account {
    scopes = ["https://www.googleapis.com/auth/compute.readonly"]
  }
}

