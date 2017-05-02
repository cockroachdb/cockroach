# Various examples that can be run against a cockroach cluster in GCE.
# A cockroach cluster should be created first by following the steps in README.md.
# To enable an example, change the number of instances on the command line. eg:
# terraform apply <flags for cockroach cluster> --var=example_block_writer_instances=1

# Number of instances for the block writer example. Set to 1 to enable the example.
# The block writer example does not support multiple instances. Expect badness if
# set greater than 1.
variable "example_block_writer_instances" {
  default = 0
}
output "example_block_writer" {
  value = "${join(",", google_compute_instance.example_block_writer.*.network_interface.0.access_config.0.assigned_nat_ip)}"
}

resource "google_compute_instance" "example_block_writer" {
  count = "${var.example_block_writer_instances}"

  name = "block-writer-${count.index}"
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

  provisioner "file" {
    source = "download_binary.sh"
    destination = "/home/ubuntu/download_binary.sh"
  }

  # This writes the filled-in supervisor template. It would be nice if we could
  # use rendered templates in the file provisioner.
  provisioner "remote-exec" {
    inline = <<FILE
echo '${data.template_file.supervisor.0.rendered}' > supervisor.conf
FILE
  }

  provisioner "remote-exec" {
    inline = [
      "sudo apt-get -y update",
      "sudo apt-get -y install supervisor",
      "sudo service supervisor stop",
      "bash download_binary.sh examples-go/block_writer ${var.block_writer_sha}",
      "mkdir -p logs",
      "if [ ! -e supervisor.pid ]; then supervisord -c supervisor.conf; fi",
      "supervisorctl -c supervisor.conf start block_writer",
    ]
  }
}
