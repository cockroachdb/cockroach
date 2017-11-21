# This Terraform configuration provides a basic topology for running Jepsen
# tests against CockroachDB. It will spin up a single Jepsen controller node
# and by default 5 instance nodes. The Jepsen controller will handle
# provisioning the instances and running the tests.

provider "google" {
  version = "~> 0.1"

  region = "${var.gce_region}"
}

provider "null" {
  version = "~> 0.1"
}

provider "template" {
  version = "~> 0.1"
}

resource "google_compute_instance" "controller" {
  count = 1
  machine_type = "${var.controller_machine_type}"
  zone = "${var.gce_zone}"
  tags = ["cockroach"]

  name = "${var.prefix}-controller"

  disk {
    image = "${var.gce_image}"
    size = "${var.controller_root_disk_size}" # GB
    type = "${var.controller_root_disk_type}"
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

  provisioner "file" {
    source = "controller.id_rsa"
    destination = "/home/ubuntu/.ssh/id_rsa"
  }
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

  provisioner "file" {
    source = "controller.id_rsa.pub"
    destination = "/home/ubuntu/.ssh/authorized_keys2"
  }
}

data "template_file" "node_list" {
  count = "1"
  template = "$${node_list}"
  vars {
    node_list = "${join("\n", google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip)}\n"
  }
}

resource "null_resource" "controller-runner" {
  count = "1"

  connection {
    user = "ubuntu"
    private_key = "${file(format("~/.ssh/%s", var.key_name))}"
    host = "${element(google_compute_instance.controller.*.network_interface.0.access_config.0.assigned_nat_ip, count.index)}"
  }

  # This writes the filled-in node_list template.
  provisioner "remote-exec" {
    inline = <<FILE
echo '${data.template_file.node_list.0.rendered}' > nodes
FILE
  }

  # Launch Jepsen controller.
  provisioner "remote-exec" {
    inline = [
      # Install test dependencies.
      "sudo apt-get -qqy update >/dev/null",
      "sudo apt-get -qqy upgrade -o Dpkg::Options::='--force-confold' >/dev/null",
      "sudo apt-get -qqy install openjdk-8-jre openjdk-8-jre-headless libjna-java git gnuplot >/dev/null",
      "chmod 600 /home/ubuntu/.ssh/id_rsa",
      # Work around JSCH auth error: https://github.com/jepsen-io/jepsen/blob/master/README.md
      "cat /home/ubuntu/nodes | xargs -n1 ssh-keyscan -t rsa >> ~/.ssh/known_hosts",
      "curl https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein > /home/ubuntu/lein",
      "chmod +x /home/ubuntu/lein",
      "cd /home/ubuntu && git clone -q https://github.com/cockroachdb/jepsen && cd jepsen && git checkout tc-nightly",
    ]
  }
}

resource "null_resource" "cockroach-runner" {
  count = "${var.num_instances}"

  connection {
    user = "ubuntu"
    private_key = "${file(format("~/.ssh/%s", var.key_name))}"
    host = "${element(google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip, count.index)}"
  }

  provisioner "file" {
    # If no binary is specified, we'll copy /dev/null (always 0 bytes) to the
    # instance. The "remote-exec" block will then overwrite that. There's no
    # such thing as conditional file copying in Terraform, so we fake it.
    source = "${coalesce(var.cockroach_binary, "/dev/null")}"
    destination = "/home/ubuntu/cockroach"
  }

  # Provision the CockroachDB instances.
  provisioner "remote-exec" {
    inline = [
      # Create file system on local SSD for the CockroachDB store and mount it.
      "sudo mkdir -p /opt",
      "sudo mkfs.ext4 -qF /dev/disk/by-id/google-local-ssd-0",
      "sudo mount -o discard,defaults /dev/disk/by-id/google-local-ssd-0 /opt",
      "sudo chown ubuntu:ubuntu /opt",
      # Update apt.
      "sudo apt-get -qqy update >/dev/null",
      "sudo apt-get -qqy upgrade -o Dpkg::Options::='--force-confold' >/dev/null",
      # Allow access to the cockroach instances from the Jepsen controller.
      "sudo cp ~/.ssh/authorized_keys2 /root/.ssh/authorized_keys2",
      # The Jepsen controller does a lot of separate ssh commands which can
      # trigger sshguard. Disable it.
      "sudo service sshguard stop",
      # Download cockroach binary, zip so that Jepsen understands it
      "mkdir -p /tmp/cockroach",
      "[ $(stat --format=%s cockroach) -ne 0 ] || curl -sfSL https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.${var.cockroach_sha} -o cockroach",
      "cp cockroach /tmp/cockroach/",
      "chmod +x /tmp/cockroach/cockroach",
      "tar -C /tmp -czf /home/ubuntu/cockroach.tgz cockroach",
    ]
  }
}
