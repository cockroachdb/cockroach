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
  template = "${file("supervisor.conf.tpl")}"
  vars {
    elb_address = "${aws_elb.elb.dns_name}:${var.cockroach_port}"
    stores = "${join(",", formatlist("ssd=%s", compact(split(",", "data,${var.aux_data_dirs}"))))}"
  }
}

# We use a null_resource to break the dependency cycle
# between aws_elb and aws_instance.
# This can be rolled back into aws_instance when https://github.com/hashicorp/terraform/issues/3999
# is addressed.
resource "null_resource" "cockroach-runner" {
  count = "${var.num_instances}"
  depends_on = [ "null_resource.cockroach-initializer" ]
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
echo '${template_file.supervisor.rendered}' > supervisor.conf
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

# The initializer is only run on the first instance and should be run only once.
# WARNING: do not "taint" the cockroach-initializer.
resource "null_resource" "cockroach-initializer" {
  count = 1
  connection {
    user = "ubuntu"
    key_file = "~/.ssh/${var.key_name}.pem"
    host = "${aws_instance.cockroach.0.public_ip}"
  }

  provisioner "file" {
    source = "download_binary.sh"
    destination = "/home/ubuntu/download_binary.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "bash download_binary.sh cockroach/cockroach ${var.cockroach_sha}",
      "mkdir -p logs",
      "./cockroach init --logtostderr=true --stores=sdd=data",
    ]
  }
}
