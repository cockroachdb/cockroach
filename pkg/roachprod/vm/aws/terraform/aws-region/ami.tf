# Search for AMI. This will fail if more than one matches "image_name".
data "aws_ami" "node_ami" {
  filter {
    name   = "name"
    values = ["${var.image_name}"]
  }
}

data "aws_ami" "node_ami_fips" {
  filter {
    name   = "name"
    values = ["${var.image_name_fips}"]
  }
}

data "aws_ami" "node_ami_arm64" {
  filter {
    name   = "name"
    values = ["${var.image_name_arm64}"]
  }
}
