data "aws_region" "current" {}
data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  project_code          = ["stg-dlk-${var.env}"]
  data_source_indicator = ["ds"]
  data_source_code      = ["100"]
  data_source_name      = ["ds100"]

  curated_data_source_indicator = ["cl"]
  curated_data_source_code      = ["100"] #verify

  bucket_type = ["raw", "refined"]

  bucket_names_values = [for values in setproduct(local.project_code, local.data_source_indicator, local.data_source_code, local.bucket_type) : join("-", values)]

  curated_bucket_names_values = [for values in setproduct(local.project_code, local.curated_data_source_indicator, local.curated_data_source_code, ["curated"]) : join("-", values)]

  lifecycle_rule = [
    {
      id      = "DeleteNonCurrentObjectVersions"
      status  = "Enabled"
      prefix  = ""
      enabled = true

      noncurrent_version_expiration = {
        days = 35
      }
    }
  ]

  glue_ds100_raw_crawler_name     = "stg-dlk-${var.env}-ds100-crawler-raw"
  glue_ds100_refined_crawler_name = "stg-dlk-${var.env}-ds100-crawler-refined"
  glue_ds100_curated_crawler_name = "stg-dlk-${var.env}-ds100-crawler-curated"


  glue_job_ds100_source_to_raw_name      = "stg-dlk-${var.env}-ds100-job-source-to-raw"
  glue_job_ds100_raw_to_refined_name     = "stg-dlk-${var.env}-ds100-job-raw-to-refined"
  glue_job_ds100_refined_to_curated_name = "stg-dlk-${var.env}-ds100-job-refined-to-curated"


  curated_db_names = [for values in setproduct(local.project_code, local.curated_data_source_indicator, local.curated_data_source_code, ["curated-db"]) : join("-", values)]
  db_names         = [for values in setproduct(local.project_code, local.data_source_name, local.bucket_type, ["db"]) : join("-", values)]

  glue_artifacts_bucket = "aws-glue-assets-${local.account_id}-${local.region}"
  account_id            = data.aws_caller_identity.current.account_id
  region                = data.aws_region.current.name


  common_tags = {
    Project   = "stg-dlk"
    ManagedBy = "Terraform"
    stg_DlkDomain = "CS_IT_ARC_Data"  ##verify
    stg_DlkPipeline = "ds100"
  }


}

  locals {
  admins_roles_policies = flatten([
    for role in local.admin_roles : [
      for policy in local.admins_managed_policy_arns : {
        role   = role
        policy = policy
      }
    ]
  ])
}

locals {
  admins_roles_tags = flatten([
    for role in local.admin_roles : [
      for tag_key, tag_values in local.lf_tags :
      {
        role = role
        tag  = tag_key
      }
    ]
  ])
}

locals {
  users_roles_policies = flatten([
    for role in local.user_roles_global_standard : [
      for policy in local.users_managed_policy_arns : {
        role   = role
        policy = policy
      }
    ]
  ])
}

locals {
  workflow_managed_policy_arns = [
    "arn:aws:iam::aws:policy/AWSGlueServiceRole"
  ]

  admins_managed_policy_arns = [
    "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin",
    "arn:aws:iam::aws:policy/AmazonAthenaFullAccess",
    "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess",
    "arn:aws:iam::aws:policy/CloudWatchLogsReadOnlyAccess",
    "arn:aws:iam::aws:policy/AWSLakeFormationCrossAccountManager"
  ]

  users_managed_policy_arns = [
    "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess",
    "arn:aws:iam::aws:policy/job-function/DataScientist",
    "arn:aws:iam::aws:policy/AmazonAthenaFullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSQuicksightAthenaAccess",
    "arn:aws:iam::aws:policy/AWSLambda_FullAccess",
    "arn:aws:iam::aws:policy/AmazonEMRFullAccessPolicy_v2",
    "arn:aws:iam::aws:policy/AmazonECS_FullAccess",
    "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSQuickSightSageMakerPolicy",
    "arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess"
  ]

  user_roles_global_standard = [
    "G_DataAnalyst",
    "G_BusIntAnalyst",
    "G_BusinessAnalyst",
    "G_DataEngineer"
  ]

  admin_roles = [
    "SystemTechnicalOwners",
    "SystemDBA"
  ]

  non_admin_roles_prod = [
    "AWSReservedSSO_DlkDataScience_bfcf0ef0a00e8e93",
    "AWSReservedSSO_DlkDataEngineer_17c388bcf0f4438c",
    "AWSReservedSSO_DlkDataAnalyst_a0b246e4561f67e8"
  ]

  non_admin_roles_sbx = [
    "AWSReservedSSO_DlkDataScience_4f239d9f9846f94c",
    "AWSReservedSSO_DlkDataEngineer_404debb20f2a030f",
    "AWSReservedSSO_DlkDataAnalyst_0a5fffe8c9452784"
  ]

  lf_tags = {
    business_unit_prh1 = [
      "1_surgical",
      "2_prosthetics",
      "3_regen",
      "4_4",
      "5_cadcam",
      "99_others"
    ]
    data_contents_patient = [
      "true",
      "false"
    ]
    data_contents_production = [
      "true",
      "false"
    ]
    data_contents_personal = [
      "true",
      "false"
    ]
    data_confidentialitylevel = [
      "public",
      "internal",
      "personal",
      "confidential",
      "strictly_confidential"
    ]
    data_zone = [
      "raw",
      "refined",
      "curated"
    ]
    sales = [
      "true",
      "false"
    ]
    complaints  = [
      "true",
      "false"
    ]
  }
}


