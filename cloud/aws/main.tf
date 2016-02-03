provider "aws" {
  region = "${var.aws_region}"
}

resource "aws_instance" "cockroach" {
  tags {
    Name = "${var.key_name}-${count.index}"
  }
  ami = "${var.aws_ami_id}"
  availability_zone = "${var.aws_availability_zone}"
  instance_type = "${var.aws_instance_type}"
  security_groups = ["${aws_security_group.default.name}"]
  key_name = "${var.key_name}"
  count = "${var.num_instances}"
}

resource "template_file" "supervisor" {
  count = "${var.num_instances}"
  template = "${file("supervisor.conf.tpl")}"
  vars {
    stores = "${var.stores}"
    # The value of the --join flag must be empty for the first node,
    # and a running node for all others. We built a list of addresses
    # shifted by one (first element is empty), then take the value at index "instance.index".
    join_address = "${element(concat(split(",", ""), aws_instance.cockroach.*.private_ip), count.index)}"
    # We need to provide one node address for the block writer.
    node_address = "${aws_instance.cockroach.0.private_ip}"
  }
}

# We use a null_resource to break the dependency cycle.
# This can be rolled back into aws_instance when https://github.com/hashicorp/terraform/issues/3999
# is addressed.
resource "null_resource" "cockroach-runner" {
  count = "${var.num_instances}"
  connection {
    user = "ubuntu"
    key_file = "~/.ssh/${var.key_name}.pem"
    host = "${element(aws_instance.cockroach.*.public_ip, count.index)}"
  }

  triggers {
    instance_ids = "${element(aws_instance.cockroach.*.id, count.index)}"
  }

  provisioner "file" {
    source = "download_binary.sh"
    destination = "/home/ubuntu/download_binary.sh"
  }

  # This writes the filled-in supervisor template. It would be nice if we could
  # use rendered templates in the file provisioner.
  provisioner "remote-exec" {
    inline = <<FILE
echo '${element(template_file.supervisor.*.rendered, count.index)}' > supervisor.conf
FILE
  }

  provisioner "remote-exec" {
    inline = [
      "sudo apt-get -y update",
      "sudo apt-get -y install supervisor",
      "sudo service supervisor stop",
      "bash download_binary.sh cockroach/cockroach ${var.cockroach_sha}",
      "mkdir -p logs",
      "if [ ! -e supervisor.pid ]; then supervisord -c supervisor.conf; fi",
      "supervisorctl -c supervisor.conf start cockroach",
    ]
  }
}
