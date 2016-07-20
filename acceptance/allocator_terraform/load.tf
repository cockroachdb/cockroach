# Load generators that can be used to fill the CockroachDB cluster with data.

variable "example_block_writer_instances" {
  default = 1
}

variable "load_duration" {
  default = "0" # unlimited
}

# Despite its name, example_block_writer actually has `photos` too. We're
# sticking to this name for compatibility with Terrafarm. Since our load
# generators are rarely CPU-bound, it's fine to have them on a single GCE
# instance.
output "example_block_writer" {
  value = "${join(",", google_compute_instance.block_writer.*.network_interface.0.access_config.0.assigned_nat_ip)}"
}

resource "google_compute_instance" "block_writer" {
  count = "${var.example_block_writer_instances}"

  name = "${var.prefix}-block-writer-${count.index + 1}"
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
    key_file = "~/.ssh/${var.key_name}"
  }

  service_account {
    scopes = ["https://www.googleapis.com/auth/compute.readonly"]
  }

  provisioner "file" {
    source = "../../cloud/gce/download_binary.sh"
    destination = "/home/ubuntu/download_binary.sh"
  }

  # This writes the filled-in supervisor template. It would be nice if we could
  # use rendered templates in the file provisioner.
  provisioner "remote-exec" {
    inline = <<FILE
echo '${template_file.supervisor.0.rendered}' > supervisor.conf
FILE
  }

  provisioner "remote-exec" {
    inline = [
      "sudo apt-get -qqy update >/dev/null",
      "sudo apt-get -qqy install supervisor >/dev/null",
      "sudo service supervisor stop",
      "bash download_binary.sh examples-go/block_writer ${var.block_writer_sha}",
      "bash download_binary.sh examples-go/photos ${var.block_writer_sha}",
      "mkdir -p logs",
      "if [ ! -e supervisor.pid ]; then supervisord -c supervisor.conf; fi",
    ]
  }
}
