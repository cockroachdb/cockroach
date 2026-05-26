# ---------------------------------------------------------------------------------------------------------------------
# Single region resources for AWS.
# All resources are created in passed-in project name and region.
# ---------------------------------------------------------------------------------------------------------------------
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.66.1"
    }
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# Module variables
# ---------------------------------------------------------------------------------------------------------------------
variable "region" { description = "AWS Region name" }
variable "image_name" {
  description = "CockroachDB base x86_64 image name"
  default     = "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-20230919"
}

variable "image_name_arm64" {
  description = "CockroachDB base arm64 image name"
  default     = "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-arm64-server-20230919"
}

# TODO: Upgrade FIPS to Ubuntu 22 when it is available.
variable "image_name_fips" {
  description = "CockroachDB base x86_64 image name"
  default     = "ubuntu-pro-fips-server/images/hvm-ssd/ubuntu-focal-20.04-amd64-pro-fips-server-20221121-7bc828d1-c072-4d33-a989-fbad50380cfb"
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
    "ami_id_arm64"   = "${data.aws_ami.node_ami_arm64.image_id}"
    "ami_id_fips"    = "${data.aws_ami.node_ami_fips.image_id}"
    "subnets" = "${zipmap(
      "${aws_subnet.region_subnets.*.availability_zone}",
      "${aws_subnet.region_subnets.*.id}"
    )}"
  }
}
