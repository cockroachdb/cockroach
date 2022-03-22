# ---------------------------------------------------------------------------------------------------------------------
# VPC peering connection between two regions.
# ---------------------------------------------------------------------------------------------------------------------
provider "aws.owner" {}
provider "aws.peer"  {}

variable "owner_vpc_info"       { type = "map", description = "VPC info for the peering owner" }
variable "peer_vpc_info"        { type = "map", description = "VPC info for the peering accepter" }
variable "label" {}

resource "aws_vpc_peering_connection" "peering_connection" {
  provider      = "aws.owner"
  vpc_id        = "${var.owner_vpc_info["vpc_id"]}"
  peer_vpc_id   = "${var.peer_vpc_info["vpc_id"]}"
  peer_region   = "${var.peer_vpc_info["region"]}"

  tags {
    Name        = "${var.label}-peering-${var.owner_vpc_info["region"]}-${var.peer_vpc_info["region"]}"
  }
}

resource "aws_vpc_peering_connection_accepter" "peering_accepter" {
  provider                  = "aws.peer"
  vpc_peering_connection_id = "${aws_vpc_peering_connection.peering_connection.id}"
  auto_accept               = true

  tags {
    Name        = "${var.label}-peering-${var.owner_vpc_info["region"]}-${var.peer_vpc_info["region"]}"
  }
}

resource "aws_route" "owner_route" {
  provider                  = "aws.owner"
  route_table_id            = "${var.owner_vpc_info["route_table_id"]}"
  destination_cidr_block    = "${var.peer_vpc_info["vpc_cidr"]}"
  vpc_peering_connection_id = "${aws_vpc_peering_connection.peering_connection.id}"
}

resource "aws_route" "peer_route" {
  provider                  = "aws.peer"
  route_table_id            = "${var.peer_vpc_info["route_table_id"]}"
  destination_cidr_block    = "${var.owner_vpc_info["vpc_cidr"]}"
  vpc_peering_connection_id = "${aws_vpc_peering_connection.peering_connection.id}"
}
