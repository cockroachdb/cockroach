# Port used for the load balancer and backends.
variable "cockroach_port" {
  default = "26257"
}

# GCE region to use.
variable "gce_region" {
  default = "us-east1"
}

# GCE zone to use.
variable "gce_zone" {
  default = "us-east1-b"
}

# GCE project name.
variable "gce_project" {
  default = "cockroach-marc"
}

# GCE account file.
variable "gce_account_file" {
  default = "~/terraform/cockroach-marc-64455dfdb138.json"
}

# GCE image name.
variable "gce_image" {
  default = "ubuntu-os-cloud/ubuntu-1510-wily-v20151021"
}

# Path to the cockroach binary.
variable "cockroach_binary" {
  default = "~/cockroach/src/github.com/cockroachdb/cockroach/cockroach"
}

# Name of the ssh key pair to use for GCE instances.
# The public key will be passed at instance creation, and the private
# key will be used by the local ssh client.
# The path is expanded to: ~/.ssh/<key_name>.pub
variable "key_name" {
  default = "gce_cockroach"
}

# Action is one of "init" or "start". init should only be specified when
# running `terraform apply` on the first node.
variable "action" {
  default = "start"
}

# Value of the --gossip flag to pass to the backends.
# This should be populated with the load balancer address.
# Make sure to populate this before changing num_instances to greater than 0.
# eg: lb=elb-893485366.us-east-1.elb.amazonaws.com:26257
variable "gossip" {
  default = "http-lb=104.196.4.96:26257"
  #default = ""
}

variable "load_balancer_address" {
  default = "104.196.4.96"
  #default = ""
}

# Number of instances to start.
variable "num_instances" {
  default = 3
}
