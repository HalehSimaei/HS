locals {
  env = var.env
}

provider "aws" {
  region = "eu-west-1"
  assume_role {
    role_arn = var.cross_account_role
  }
}

terraform {
  required_version = "1.5.0"

  backend "s3" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "4.55.0"
    }
  }
}
