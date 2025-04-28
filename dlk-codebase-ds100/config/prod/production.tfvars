env                  = "prod"
cross_account_role   = "arn:aws:iam::286719176505:role/stg-dlk-devops"
region               = "eu-west-1"
vpc_id               = "vpc-0fd3347ae8e67f307"
subnets_private      = ["subnet-0b240fecf46e74bec", "subnet-0203609473aa50b32"]
subnets_public       = ["subnet-07c1c384ad2aebe15", "subnet-072eb63a899780d41"]
external_kms_key_arn = "arn:aws:kms:eu-west-1:286719176505:key/70aa0c68-0398-4f16-8fda-872b3df4c0a4"
grant_type           = "client_credentials"
client_id            = "SKILL-OPENID-CLIENT-REPORT"
glue_job_az          = "eu-west-1a"
s3_bucket_code_artifacts_name = "stg-dlk-prod-code-artifacts"
s3_bucket_glue_job_temp_name  = "stg-dlk-prod-glue-job-temporary-files"
s3_wheels_bucket_id           = "stg-dlk-prod-wheels"
ds100_glue_job_subnet  = "subnet-0d2e905b8b0fa3b92"
helpscout_app_id              = "VsL8CAtgOkaxZwGaMLf6OQu4okBGMJ86"
helpscout_api_base_url        = "https://api.helpscout.net/v2/"
helpscout_app_secret          = "4VdneVpUYtUoB0g57qN5omjNcSzOMbvt"



##CIDR blocks and vpc endpoints?





