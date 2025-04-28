variable "data_source" {
  type        = string
  description = "Data source name"
  default     = "ds-100"
}

variable "env" {
  type        = string
  description = "the environment of deployment name"
}

variable "cross_account_role" {
  description = "role to assume for target account"
}

variable "region" {
  type        = string
  description = "The region where data lake resides."
  default     = "eu-west-1"
}

variable "ds100_source_endpoint_secrets" {
  type        = string
  description = "secret name for ds100"
  default     = "stg/dlk/clearcom/ds100/secrets" #change
}


variable "external_kms_key_arn" {
  type        = string
  description = "External kms key to use for dlk"
}


variable "vpc_id" {
  type        = string
  description = "id for the vpc"
}

variable "subnets_private" {
  type        = list(string)
  description = "list of private subnets for the appflow nlb"
}

variable "subnets_public" {
  type        = list(string)
  description = "list of public subnets for the sftp"
}

variable "grant_type" {
  type        = string
  description = "grant type for ds100 lmabda ingestion"
}

variable "client_id" {
  type        = string
  description = "client id for ds100 lmabda ingestion"
}

variable "glue_job_az" {
  type        = string
  description = "The AZ where to deploy the Glue Job"
}

variable "glue_security_configuration_name" {
  type    = string
  default = "dlk-glue-sec-config"
}

variable "s3_bucket_code_artifacts_name" {
  type    = string
}

variable "s3_bucket_glue_job_temp_name" {
  type    = string
}

variable "s3_wheels_bucket_id" {
  type        = string
  description = "The name of s3 bucket which stores common python wheels"
}

variable "ds100_glue_job_subnet" {
  type        = string
  description = "The subnet where to deploy the Glue Job in ds100"
}

variable "helpscout_app_id" {
  type        = string
  description = "Helpscout app id"
}

variable "helpscout_api_base_url" {
  type        = string
  description = "Helpscout app base url"
}

variable "helpscout_app_secret" {
  type        = string
  description = "Helpscout app secret"
}


