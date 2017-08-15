provider "google" {
  version = "~> 0.1"

  region = "${var.gce_region}"
}

provider "null" {
  version = "~> 0.1"
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

# Set up CockroachDB nodes.
resource "null_resource" "cockroach-runner" {
  count = "${var.num_instances}"

  connection {
    user = "ubuntu"
    private_key = "${file(format("~/.ssh/%s", var.key_name))}"
    host = "${element(google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip, count.index)}"
  }

  provisioner "file" {
    source = "${var.cockroach_binary}"
    destination = "/home/ubuntu/cockroach"
  }

  # Launch CockroachDB.
  provisioner "remote-exec" {
      inline = [
      "chmod +x cockroach",
      # Create file system on local SSD for the CockroachDB store and mount it.
      "sudo mkdir /mnt/data0",
      "sudo mkfs.ext4 -qF /dev/disk/by-id/google-local-ssd-0",
      "sudo mount -o discard,defaults /dev/disk/by-id/google-local-ssd-0 /mnt/data0",
      "sudo chown ubuntu:ubuntu /mnt/data0",
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
