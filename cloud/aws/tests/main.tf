# Run the sql logic test suite on AWS.
# Prerequisites:
# - AWS account credentials file as specified in cockroach-prod/terraform/aws/README.md
# - sqllogic test repo cloned
#
# Run with:
# $ terraform apply
#
# Tear down AWS resources using:
# $ terraform destroy
#
# The used logic tests are tarred and gzipped before launching the instance.
# Test are sharded by subdirectory (see variables.tf for details), with one
# instance handling each subdirectory.
# The latest sql.test binary is fetched from S3.
#
# Monitor the output of the tests by running:
# $ ssh -i ~/.ssh/cockroach.pem ubuntu@<instance> tail -F test.STDOUT

provider "aws" {
  region = "${var.aws_region}"
}

output "instance" {
  value = "${join(",", aws_instance.sql_logic_test.*.public_dns)}"
}

resource "aws_instance" "sql_logic_test" {
  tags {
    Name = "${var.key_name}-sql-logic-test-${count.index}"
  }
  depends_on = ["null_resource.sql_tarball"]

  ami = "${var.aws_ami_id}"
  availability_zone = "${var.aws_availability_zone}"
  instance_type = "${var.aws_instance_type}"
  security_groups = ["${aws_security_group.default.name}"]
  key_name = "${var.key_name}"
  count = "${length(split(",", var.sqllogictest_subdirectories))}"

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

resource "null_resource" "sql_tarball" {
  count = "${length(split(",", var.sqllogictest_subdirectories))}"
  provisioner "local-exec" {
    command = "tar cfz tarball${count.index}.tgz -C ${var.sqllogictest_repo} ${element(split(",", var.sqllogictest_subdirectories),count.index)}"
  }
}

resource "aws_security_group" "default" {
  name = "${var.key_name}-sqltest-security-group"

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
