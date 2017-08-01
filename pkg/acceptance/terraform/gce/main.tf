provider "google" {
  region = "${var.gce_region}"
}

resource "google_compute_instance" "cockroach" {
  count = "${var.num_instances}"

  name = "${var.prefix}-cockroach-${count.index + 1}"
  machine_type = "${var.cockroach_machine_type}"
  zone = "${var.gce_zone}"
  tags = ["cockroach"]

  disk {
    image = "${var.gce_image}"
    size = "${var.cockroach_root_disk_size}" # GB
    type = "${var.cockroach_root_disk_type}"
  }

  # Add a local SSD for CockroachDB files. Under sustained I/O, something seems
  # to throttle performance when using persistent storage types. So, we have to
  # use local SSDs.
  disk {
    # Local SSDs are always 375 GB:
    # https://cloud.google.com/compute/docs/disks/local-ssd#create_local_ssd
    type = "local-ssd"
    scratch = true
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
    scopes = ["https://www.googleapis.com/auth/compute.readonly", "https://www.googleapis.com/auth/devstorage.read_write"]
  }
}
