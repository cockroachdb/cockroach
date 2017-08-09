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

data "external" "git" {
  program = ["go", "run", "../common/git.go"]
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
    source = "../common/nodectl"
    destination = "/home/ubuntu/nodectl"
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
      "chmod +x nodectl",
      # Create file system on local SSD for the CockroachDB store and mount it.
      "sudo mkdir /mnt/data0",
      "sudo mkfs.ext4 -qF /dev/disk/by-id/google-local-ssd-0",
      "sudo mount -o discard,defaults /dev/disk/by-id/google-local-ssd-0 /mnt/data0",
      "sudo chown ubuntu:ubuntu /mnt/data0",
      # Install test dependencies.
      "export CLOUD_SDK_REPO=\"cloud-sdk-$(lsb_release -c -s)\"",
      "echo \"deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main\" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list",
      "curl -sS https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -",
      "sudo apt-get -qqy update >/dev/null",
      "sudo apt-get -qqy install google-cloud-sdk >/dev/null",
      # Send logs to local SSD.
      "mkdir /mnt/data0/logs",
      "ln -sf /mnt/data0/logs logs",
      # Install CockroachDB.
      "[ -f cockroach ] || curl -sfSL https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.${data.external.git.result.SHA} -o cockroach",
      "chmod +x cockroach",
      # Install load generators.
      "curl -sfSL https://edge-binaries.cockroachdb.com/examples-go/block_writer.${var.block_writer_sha} -o block_writer",
      "chmod +x block_writer",
      "curl -sfSL https://edge-binaries.cockroachdb.com/examples-go/photos.${var.photos_sha} -o photos",
      "chmod +x photos",
    ]
  }
}
