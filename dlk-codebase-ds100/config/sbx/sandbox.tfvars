env                  = "sbx"
vpc_id               = "vpc-0bf7f1605b50878de"
subnets_private      = ["subnet-0adcd7e7fbb90cd5b", "subnet-098d380b36e5973f4"]
subnets_public       = ["subnet-02801b07d8ee295f2", "subnet-02058c29732497422"]
cross_account_role   = "arn:aws:iam::816247855850:role/stg-dlk-devops"
external_kms_key_arn = "arn:aws:kms:eu-west-1:816247855850:key/396cd8ff-4b3d-4b17-9df4-9449185fdd2e"
grant_type           = "client_credentials"
client_id            = "STRUAT-OPENID-CLIENT-REPORT"
region               = "eu-west-1"
glue_job_az          = "eu-west-1b"
##added by Saro 
s3_bucket_code_artifacts_name = "stg-dlk-sbx-code-artifacts"
s3_bucket_glue_job_temp_name  = "stg-dlk-sbx-glue-job-temporary-files"
s3_wheels_bucket_id           = "stg-dlk-sbx-wheels"
ds100_glue_job_subnet         = "subnet-098d380b36e5973f4"
helpscout_app_id              = "VsL8CAtgOkaxZwGaMLf6OQu4okBGMJ86"
helpscout_api_base_url        = "https://api.helpscout.net/v2/"
helpscout_app_secret          = "4VdneVpUYtUoB0g57qN5omjNcSzOMbvt"




##CIDR blocks and vpc endpoints?



