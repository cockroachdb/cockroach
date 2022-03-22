# ---------------------------------------------------------------------------------------------------------------------
# TERRAFORM SETTINGS
# ---------------------------------------------------------------------------------------------------------------------
terraform {
  required_version = ">= 0.11.8"
  backend "s3" {
    key            = "terraform/roachprod"
    bucket         = "roachprod-cloud-state"
    region         = "us-east-2"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# Variable names that should not change beyond initial config.
# ---------------------------------------------------------------------------------------------------------------------
locals {
  account_number = "541263489771"
  label          = "roachprod"
}

# ---------------------------------------------------------------------------------------------------------------------
# AWS
# ---------------------------------------------------------------------------------------------------------------------

provider "aws" {
  alias   = "roachprod-ap-northeast-1"
  region  = "ap-northeast-1"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-ap-northeast-1" {
  providers {
    aws  = "aws.roachprod-ap-northeast-1"
  }
  region = "ap-northeast-1"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-ap-northeast-2"
  region  = "ap-northeast-2"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-ap-northeast-2" {
  providers {
    aws  = "aws.roachprod-ap-northeast-2"
  }
  region = "ap-northeast-2"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-ap-south-1"
  region  = "ap-south-1"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-ap-south-1" {
  providers {
    aws  = "aws.roachprod-ap-south-1"
  }
  region = "ap-south-1"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-ap-southeast-1"
  region  = "ap-southeast-1"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-ap-southeast-1" {
  providers {
    aws  = "aws.roachprod-ap-southeast-1"
  }
  region = "ap-southeast-1"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-ap-southeast-2"
  region  = "ap-southeast-2"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-ap-southeast-2" {
  providers {
    aws  = "aws.roachprod-ap-southeast-2"
  }
  region = "ap-southeast-2"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-ca-central-1"
  region  = "ca-central-1"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-ca-central-1" {
  providers {
    aws  = "aws.roachprod-ca-central-1"
  }
  region = "ca-central-1"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-eu-central-1"
  region  = "eu-central-1"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-eu-central-1" {
  providers {
    aws  = "aws.roachprod-eu-central-1"
  }
  region = "eu-central-1"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-eu-west-1"
  region  = "eu-west-1"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-eu-west-1" {
  providers {
    aws  = "aws.roachprod-eu-west-1"
  }
  region = "eu-west-1"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-eu-west-2"
  region  = "eu-west-2"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-eu-west-2" {
  providers {
    aws  = "aws.roachprod-eu-west-2"
  }
  region = "eu-west-2"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-eu-west-3"
  region  = "eu-west-3"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-eu-west-3" {
  providers {
    aws  = "aws.roachprod-eu-west-3"
  }
  region = "eu-west-3"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-sa-east-1"
  region  = "sa-east-1"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-sa-east-1" {
  providers {
    aws  = "aws.roachprod-sa-east-1"
  }
  region = "sa-east-1"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-us-east-1"
  region  = "us-east-1"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-us-east-1" {
  providers {
    aws  = "aws.roachprod-us-east-1"
  }
  region = "us-east-1"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-us-east-2"
  region  = "us-east-2"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-us-east-2" {
  providers {
    aws  = "aws.roachprod-us-east-2"
  }
  region = "us-east-2"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-us-west-1"
  region  = "us-west-1"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-us-west-1" {
  providers {
    aws  = "aws.roachprod-us-west-1"
  }
  region = "us-west-1"
  source = "aws-region"
  label  = "roachprod"
}

provider "aws" {
  alias   = "roachprod-us-west-2"
  region  = "us-west-2"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_roachprod-us-west-2" {
  providers {
    aws  = "aws.roachprod-us-west-2"
  }
  region = "us-west-2"
  source = "aws-region"
  label  = "roachprod"
}


module "vpc_peer_roachprod-ap-northeast-1-roachprod-ap-northeast-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-ap-northeast-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ap-northeast-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-ap-south-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-ap-south-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ap-south-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-ap-southeast-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-ap-southeast-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ap-southeast-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-ap-southeast-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-ap-southeast-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ap-southeast-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-ca-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-ca-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ca-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-eu-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-eu-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-eu-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-eu-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-eu-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-eu-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-eu-west-3" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-eu-west-3"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-3.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-sa-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-sa-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-sa-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-1-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-1"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-ap-south-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-ap-south-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ap-south-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-ap-southeast-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-ap-southeast-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ap-southeast-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-ap-southeast-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-ap-southeast-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ap-southeast-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-ca-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-ca-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ca-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-eu-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-eu-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-eu-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-eu-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-eu-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-eu-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-eu-west-3" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-eu-west-3"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-3.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-sa-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-sa-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-sa-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-northeast-2-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-northeast-2"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-northeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-ap-southeast-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-ap-southeast-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ap-southeast-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-ap-southeast-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-ap-southeast-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ap-southeast-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-ca-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-ca-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ca-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-eu-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-eu-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-eu-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-eu-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-eu-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-eu-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-eu-west-3" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-eu-west-3"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-3.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-sa-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-sa-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-sa-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-south-1-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-south-1"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-south-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-ap-southeast-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-ap-southeast-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ap-southeast-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-ca-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-ca-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ca-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-eu-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-eu-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-eu-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-eu-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-eu-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-eu-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-eu-west-3" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-eu-west-3"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-3.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-sa-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-sa-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-sa-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-1-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-1"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-2-roachprod-ca-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-2"
    aws.peer     = "aws.roachprod-ca-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-ca-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-2-roachprod-eu-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-2"
    aws.peer     = "aws.roachprod-eu-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-2-roachprod-eu-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-2"
    aws.peer     = "aws.roachprod-eu-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-2-roachprod-eu-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-2"
    aws.peer     = "aws.roachprod-eu-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-2-roachprod-eu-west-3" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-2"
    aws.peer     = "aws.roachprod-eu-west-3"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-3.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-2-roachprod-sa-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-2"
    aws.peer     = "aws.roachprod-sa-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-sa-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-2-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-2"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-2-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-2"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-2-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-2"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ap-southeast-2-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ap-southeast-2"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ap-southeast-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ca-central-1-roachprod-eu-central-1" {
  providers {
    aws.owner    = "aws.roachprod-ca-central-1"
    aws.peer     = "aws.roachprod-eu-central-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ca-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-central-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ca-central-1-roachprod-eu-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ca-central-1"
    aws.peer     = "aws.roachprod-eu-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ca-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ca-central-1-roachprod-eu-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ca-central-1"
    aws.peer     = "aws.roachprod-eu-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ca-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ca-central-1-roachprod-eu-west-3" {
  providers {
    aws.owner    = "aws.roachprod-ca-central-1"
    aws.peer     = "aws.roachprod-eu-west-3"
  }
  owner_vpc_info = "${module.aws_roachprod-ca-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-3.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ca-central-1-roachprod-sa-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ca-central-1"
    aws.peer     = "aws.roachprod-sa-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ca-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-sa-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ca-central-1-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-ca-central-1"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ca-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ca-central-1-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-ca-central-1"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ca-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ca-central-1-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-ca-central-1"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-ca-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-ca-central-1-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-ca-central-1"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-ca-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-central-1-roachprod-eu-west-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-central-1"
    aws.peer     = "aws.roachprod-eu-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-central-1-roachprod-eu-west-2" {
  providers {
    aws.owner    = "aws.roachprod-eu-central-1"
    aws.peer     = "aws.roachprod-eu-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-central-1-roachprod-eu-west-3" {
  providers {
    aws.owner    = "aws.roachprod-eu-central-1"
    aws.peer     = "aws.roachprod-eu-west-3"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-3.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-central-1-roachprod-sa-east-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-central-1"
    aws.peer     = "aws.roachprod-sa-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-sa-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-central-1-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-central-1"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-central-1-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-eu-central-1"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-central-1-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-central-1"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-central-1-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-eu-central-1"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-central-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-1-roachprod-eu-west-2" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-1"
    aws.peer     = "aws.roachprod-eu-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-1-roachprod-eu-west-3" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-1"
    aws.peer     = "aws.roachprod-eu-west-3"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-3.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-1-roachprod-sa-east-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-1"
    aws.peer     = "aws.roachprod-sa-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-sa-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-1-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-1"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-1-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-1"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-1-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-1"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-1-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-1"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-2-roachprod-eu-west-3" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-2"
    aws.peer     = "aws.roachprod-eu-west-3"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-eu-west-3.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-2-roachprod-sa-east-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-2"
    aws.peer     = "aws.roachprod-sa-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-sa-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-2-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-2"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-2-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-2"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-2-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-2"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-2-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-2"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-3-roachprod-sa-east-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-3"
    aws.peer     = "aws.roachprod-sa-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-3.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-sa-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-3-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-3"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-3.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-3-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-3"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-3.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-3-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-3"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-3.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-eu-west-3-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-eu-west-3"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-eu-west-3.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-sa-east-1-roachprod-us-east-1" {
  providers {
    aws.owner    = "aws.roachprod-sa-east-1"
    aws.peer     = "aws.roachprod-us-east-1"
  }
  owner_vpc_info = "${module.aws_roachprod-sa-east-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-sa-east-1-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-sa-east-1"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-sa-east-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-sa-east-1-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-sa-east-1"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-sa-east-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-sa-east-1-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-sa-east-1"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-sa-east-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-us-east-1-roachprod-us-east-2" {
  providers {
    aws.owner    = "aws.roachprod-us-east-1"
    aws.peer     = "aws.roachprod-us-east-2"
  }
  owner_vpc_info = "${module.aws_roachprod-us-east-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-east-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-us-east-1-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-us-east-1"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-us-east-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-us-east-1-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-us-east-1"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-us-east-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-us-east-2-roachprod-us-west-1" {
  providers {
    aws.owner    = "aws.roachprod-us-east-2"
    aws.peer     = "aws.roachprod-us-west-1"
  }
  owner_vpc_info = "${module.aws_roachprod-us-east-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-1.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-us-east-2-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-us-east-2"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-us-east-2.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}

module "vpc_peer_roachprod-us-west-1-roachprod-us-west-2" {
  providers {
    aws.owner    = "aws.roachprod-us-west-1"
    aws.peer     = "aws.roachprod-us-west-2"
  }
  owner_vpc_info = "${module.aws_roachprod-us-west-1.vpc_info}"
  peer_vpc_info  = "${module.aws_roachprod-us-west-2.vpc_info}"

  label          = "roachprod"
  source         = "aws-vpc-peer"
}


output "regions" {
  value = "${list(
    "${module.aws_roachprod-ap-northeast-1.region_info}",
    "${module.aws_roachprod-ap-northeast-2.region_info}",
    "${module.aws_roachprod-ap-south-1.region_info}",
    "${module.aws_roachprod-ap-southeast-1.region_info}",
    "${module.aws_roachprod-ap-southeast-2.region_info}",
    "${module.aws_roachprod-ca-central-1.region_info}",
    "${module.aws_roachprod-eu-central-1.region_info}",
    "${module.aws_roachprod-eu-west-1.region_info}",
    "${module.aws_roachprod-eu-west-2.region_info}",
    "${module.aws_roachprod-eu-west-3.region_info}",
    "${module.aws_roachprod-sa-east-1.region_info}",
    "${module.aws_roachprod-us-east-1.region_info}",
    "${module.aws_roachprod-us-east-2.region_info}",
    "${module.aws_roachprod-us-west-1.region_info}",
    "${module.aws_roachprod-us-west-2.region_info}"
  )}"
}

