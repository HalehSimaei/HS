data "aws_acm_certificate" "straumann" {
  domain   = "straumann.com"
  statuses = ["ISSUED"]
}


data "terraform_remote_state" "core_infrastructure" {
  backend = "s3"
  config = {
    bucket         = "stg-dlk-tf-states"
    key            = "stg-dlk-${var.env}/terraform.tfstate"
    region         = local.region
    dynamodb_table = "stg-dlk-${var.env}-tf-state-lock"
  }
}