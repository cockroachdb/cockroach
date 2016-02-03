# Run the sql logic test suite on AWS.
# Prerequisites:
# - AWS account credentials file as specified in cockroach-prod/terraform/aws/README.md
#
# Run with:
# $ terraform apply
#
# Tear down AWS resources using:
# $ terraform destroy

provider "aws" {
  region = "${var.aws_region}"
}

output "instance" {
  value = "${join(",", aws_instance.stress.*.public_dns)}"
}

resource "aws_instance" "stress" {
  tags {
    Name = "${var.key_name}-stress-${count.index}"
  }

  ami = "${var.aws_ami_id}"
  availability_zone = "${var.aws_availability_zone}"
  instance_type = "${var.aws_instance_type}"
  security_groups = ["${aws_security_group.default.name}"]
  key_name = "${var.key_name}"
  count = 1

  connection {
    user = "ubuntu"
    key_file = "~/.ssh/${var.key_name}.pem"
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
      "if [ ! -e supervisor.pid ]; then supervisord -c supervisor.conf; fi",
      "supervisorctl -c supervisor.conf start stress",
    ]
  }
}

resource "aws_security_group" "default" {
  name = "${var.key_name}-stress-security-group"

  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port = "${var.supervisor_port}"
    to_port = "${var.supervisor_port}"
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
