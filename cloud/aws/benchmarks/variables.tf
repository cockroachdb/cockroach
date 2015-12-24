# Sha of the binary to pull down. If none, the latest is fetched.
variable "benchmarks_sha" {
  default = ""
}

# Port used by supervisord.
variable "supervisor_port" {
  default = "9001"
}

# AWS region to use. WARNING: changing this will break the AMI ID.
variable "aws_region" {
  default = "us-east-1"
}

# AWS availability zone. Make sure it exists for your account.
variable "aws_availability_zone" {
  default = "us-east-1b"
}

# AWS image ID. The default is valid for region "us-east-1".
# This is an ubuntu image with HVM.
variable "aws_ami_id" {
  default = "ami-1c552a76"
}

# AWS instance type. This may affect valid AMIs.
variable "aws_instance_type" {
  default = "c4.2xlarge"
}

# Name of the ssh key pair for this AWS region. Your .pem file must be:
# ~/.ssh/<key_name>.pem
variable "key_name" {
  default = "cockroach"
}
