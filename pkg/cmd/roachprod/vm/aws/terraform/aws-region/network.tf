locals {
  # AWS VPCs are per-region, and the default ones all have the same CIDR range so cannot
  # be connected.
  # We need to create one VPC per region with known CIDR ranges.
  # We create a single private class-A network: 10.0.0.0/8
  # Each region is a fixed class-B network:     10.<region id>.0.0/12
  # Each zone is a fixed class-C network:       10.<region id>.<zone id 4 bits>0.0/20
  #
  # Note: this allows for a maximum of 4095 (minus reserved IPs) VMs per zone.
  #
  # The region ID and zone ID mapping must **NEVER** change during the lifetime of a cluster.
  # If a region or zone no longer exists, comment it but do not reuse the number.
  # If a region or zone is added, use the next available number.
  region_number = {
    ap-northeast-1  = 0,
    ap-northeast-2  = 1,
    ap-south-1      = 2,
    ap-southeast-1  = 3,
    ap-southeast-2  = 4,
    ca-central-1    = 5,
    eu-central-1    = 6,
    eu-west-1       = 7,
    eu-west-2       = 8,
    eu-west-3       = 9,
    sa-east-1       = 10,
    us-east-1       = 11,
    us-east-2       = 12,
    us-west-1       = 13,
    us-west-2       = 14,
  }

  zone_number = {
    a = 0,
    b = 1,
    c = 2,
    d = 3,
    e = 4,
    f = 5,
    g = 6,
    h = 7,
    i = 8,
    j = 9,
    k = 10,
  }

  # Computed variable: number of availability zones (and subnets) in this region.
  num_zones = "${length(data.aws_availability_zones.available.names)}"
}

# List of all availability zones in this region.
data "aws_availability_zones" "available" {}

# Details for each availability zone.
data "aws_availability_zone" "zone_detail" {
  count = "${local.num_zones}"
  name = "${data.aws_availability_zones.available.names[count.index]}"
}

# One VPC per region, with CIDR 10.<region ID>.0.0/8.
resource "aws_vpc" "region_vpc" {
  cidr_block           = "${cidrsubnet("10.0.0.0/8", 8, local.region_number[var.region])}"
  enable_dns_hostnames = true
  tags {
    Name               = "${var.label}-vpc-${var.region}"
  }
}

# Gateway for the VPC.
resource "aws_internet_gateway" "gateway" {
  vpc_id = "${aws_vpc.region_vpc.id}"
}

# Route table for the VPC (automatically associated with all subnets).
data "aws_route_table" "region_route_table" {
  vpc_id = "${aws_vpc.region_vpc.id}"
}

# Route all traffic through internet gateway (VPC CIDRs are separate routes).
resource "aws_route" "internet_route" {
  route_table_id         = "${data.aws_route_table.region_route_table.id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.gateway.id}"
}

# List of subnets. One for each availability zone, with CIDR 10.<region ID>.<zone ID>.0/16.
resource "aws_subnet" "region_subnets" {
  count             = "${local.num_zones}"
  availability_zone = "${data.aws_availability_zone.zone_detail.*.name[count.index]}"
  vpc_id            = "${aws_vpc.region_vpc.id}"
  cidr_block        = "${cidrsubnet(aws_vpc.region_vpc.cidr_block, 4, local.zone_number[data.aws_availability_zone.zone_detail.*.name_suffix[count.index]])}"
  tags {
    Name        = "${var.label}-subnet-${data.aws_availability_zone.zone_detail.*.name[count.index]}"
  }
}

# Security group for the VPC.
# WARNING: do not define any rules inside the "aws_security_group" stanza, use "aws_security_group_rule" instead.
resource "aws_security_group" "region_security_group" {
  name        = "${var.label}-group-${var.region}"
  description = "Security group for region ${var.region}"
  vpc_id      = "${aws_vpc.region_vpc.id}"
}

# Egress: allow all.
resource "aws_security_group_rule" "allow_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "all"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = "${aws_security_group.region_security_group.id}"
  description       = "Egress"
}

# Ingress: allow all.
resource "aws_security_group_rule" "allow_ingress" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "all"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = "${aws_security_group.region_security_group.id}"
  description       = "Ingress"
}


# Ingress: allow from all other VPCs.
resource "aws_security_group_rule" "allow_vpc_ingress" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "all"
  cidr_blocks       = ["10.0.0.0/8"]
  security_group_id = "${aws_security_group.region_security_group.id}"
  description       = "Inter-VPC traffic"
}

# Ingress: allow SSH from everywhere.
resource "aws_security_group_rule" "allow_ssh_ingress" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = "${aws_security_group.region_security_group.id}"
  description       = "SSH Access"
}
