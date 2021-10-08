# ---------------------------------------------------------------------------------------------------------------------
# Single region resources for GCP.
# All resources are created in passed-in project name and region.
# ---------------------------------------------------------------------------------------------------------------------
provider "aws" {}

# ---------------------------------------------------------------------------------------------------------------------
# Module variables
# ---------------------------------------------------------------------------------------------------------------------
variable "region"                 { description = "AWS Region name" }
variable "image_name"             { 
    description = "CockroachDB base image name" 
    default = "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-20210325"
}

variable "label" {
    description = "Used as the resource name prefix."
}

# ---------------------------------------------------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------------------------------------------------

# Contains all necessary information to configure VPC peering.
output "vpc_info" {
  value = {
    "region"         = "${var.region}"
    "vpc_id"         = "${aws_vpc.region_vpc.id}"
    "vpc_cidr"       = "${aws_vpc.region_vpc.cidr_block}"
    "security_group" = "${aws_security_group.region_security_group.id}"
    "route_table_id" = "${data.aws_route_table.region_route_table.id}"
  }
}

output "region_info" {
  value = {
    "region"         = "${var.region}"
    "security_group" = "${aws_security_group.region_security_group.id}"
    "ami_id"         = "${data.aws_ami.node_ami.image_id}"
    "subnets"        = "${zipmap(
        "${aws_subnet.region_subnets.*.availability_zone}", 
        "${aws_subnet.region_subnets.*.id}"
    )}"
  }
}
