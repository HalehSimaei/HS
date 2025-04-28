
###  ds100 Source to Raw Flow    ####

############## STEP FUNCTIONS ##################
# ------------- IAM Role for Step Function -------------

## SFN Raw IAM Role
module "ds100_raw_sfn_role" {
  source = "./dlk-shared-modules/modules/iam"

  role_name = "stg-dlk-${var.env}-ds100-raw-sfn-role"
  assume_role_policy = templatefile("${path.module}/templates/arn_like_assume_role_policy.tpl", {
    service    = "states.amazonaws.com"
    account_id = local.account_id 
  })
  policy_file_path   = "artifacts/ds100/step_function/policies/ds100_s2r_sfn_policy.tpl" 
  policy_name        = "stg-dlk-${var.env}-ds100-raw-sfn-policy"
  policy_description = "policy for ds100 raw step function"
  policy_vars = {
    glue_job_arn = module.ds100-job-source-to-raw.glue_job_arn
    crawler_name = module.glue_ds100_crawler_raw.glue_crawler_name
    region       = var.region
    account_id   = local.account_id  
  }
  tags = local.common_tags
}

## SFN Raw ##
module "ds100_raw_sfn" {
  source = "./dlk-shared-modules/modules/step_function"

  name                 = "stg-dlk-${var.env}-ds100-raw"
  use_existing_role    = false
  role_arn             = module.ds100_raw_sfn_role.iam_role_arn
  definition_file_path = "artifacts/ds100/step_function/raw/sfn_raw_definition.tpl"  
  sfn_variables = {
    glue_job_name   = module.ds100-job-source-to-raw.glue_job_name
    crawler_name   = module.glue_ds100_crawler_raw.glue_crawler_name
  }

  # Logging
  create_log_group                = true
  enable_xray_tracing             = false
  cloudwatch_log_group_kms_key_id = var.external_kms_key_arn

  logging_configuration = {
    include_execution_data = true
    level                  = "ALL"
  }

  # Artifacts
  store_on_s3 = true
  s3_bucket   = var.s3_bucket_code_artifacts_name
  s3_prefix   = "artifacts/step_function_ds100_raw/zip"

  source_path = "${path.root}/artifacts/ds100/step_function/raw/"
  output_path = "${path.root}/artifacts/ds100/step_function/"

  tags = local.common_tags
}

#### Create Glue Database Raw #####
module "glue_db_raw" {
  source = "./dlk-shared-modules/modules/glue"
  # KMS key for the catalog encryption settings
  kms_key_arn = var.external_kms_key_arn

  create_glue_catalog_db      = true
  create_glue_crawler         = false
  create_glue_connection      = false
  glue_catalog_db_name        = "stg-dlk-${var.env}-${var.data_source}-raw-db"
  glue_catalog_db_description = "Glue catalog db with metadata about ingested data into raw bucket."
  glue_location_uri           = "s3://stg-dlk-${var.env}-${var.data_source}-raw" 

  ### TAGS
  tags = local.common_tags
}

## Role to enable access through Lake formation
## This role is used by Glue jobs and crawlers to access the data lake resources.
## It is assumed by Glue jobs and crawlers to access the data lake resources.
## It is used to access the data lake resources through Lake formation.
## it's add on 2025-04-03 by Saro
module "ds100_lf_role_raw" {
  source    = "./dlk-shared-modules//modules/iam"
   role_name = "stg-dlk-${var.env}-ds-100-raw-lf-role"
  assume_role_policy = templatefile("${path.module}/templates/assume_role_policy_no_condition.tpl",
   {
     service    = "glue.amazonaws.com"
  })
   policy_name        = "stg-dlk-${var.env}-ds100-raw-lf-role-policy"
  policy_file_path   = "templates/glue_raw_data_catalog_policy.tpl"
  policy_description = "policy for ds100 glue raw processing"
   tags               = local.common_tags
  policy_vars = {
     env              = var.env
    kms_key_arn      = var.external_kms_key_arn
    bucket_artifacts = var.s3_bucket_code_artifacts_name
    ds_id            = 100
    account_id       = local.account_id
    raw_db_name      = module.glue_db_raw.glue_catalog_db_name
    region_name      = local.region
  }
 }


########## CRAWLER FOR RAW ZONE ##########

# ------------- IAM Role for Glue Crawler -------------

module "glue_ds100_crawler_role_raw" {
  source    = "./dlk-shared-modules/modules/iam"
  role_name = "stg-dlk-${var.env}-ds100-raw-crawler-role"
  assume_role_policy = templatefile("${path.module}/templates/arn_like_assume_role_policy.tpl", {
    service    = "glue.amazonaws.com"
    account_id = local.account_id  
  })
  policy_name        = "stg-dlk-${var.env}-ds100-raw-crawler-policy"
  policy_file_path   = "templates/glue_crawler_role_policy.json"
  policy_description = "ds100 Raw Glue Crawler Policy"
  tags               = local.common_tags
  policy_vars = {
    datalake_zone             = "raw"
    env                       = var.env
    datasource                = "ds-100"
    kms_key_arn               = var.external_kms_key_arn
    role_name                 = "stg-dlk-${var.env}-ds100-raw-crawler-role"
    log_group_sec_config_name = var.glue_security_configuration_name
    region                    = local.region
    account_id                = local.account_id 
  }
}

### AWSGlueServiceRole managed role attachment for glue crawler
resource "aws_iam_role_policy_attachment" "ds100_glue_managed_service_role_policy_raw" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = module.glue_ds100_crawler_role_raw.iam_role_name
}



# ------------- Glue Crawler for Raw Zone -------------

module "glue_ds100_crawler_raw" {
  source = "./dlk-shared-modules/modules/glue"
  # KMS key for the catalog encryption settings
  kms_key_arn                 = var.external_kms_key_arn
  glue_crawler_name           = local.glue_ds100_raw_crawler_name
  glue_crawler_description    = "Crawler for ds100 s3 bucket"
  glue_crawler_role           = module.glue_ds100_crawler_role_raw.iam_role_arn
  glue_crawler_configuration  = <<EOF
{
  "Version": 1.0,
  "Grouping": {
    "TableGroupingPolicy": "CombineCompatibleSchemas",
    "TableLevelConfiguration": 3
  },
  "CrawlerOutput" : {
    "Partitions" : { "AddOrUpdateBehavior" : "InheritFromTable" }
  }
}
EOF
  glue_security_configuration = var.glue_security_configuration_name
  glue_lake_formation_configuration = ({
    account_id                     = local.account_id
    use_lake_formation_credentials = true
  })
  s3_target = [
    {
    path = "s3://stg-dlk-${var.env}-ds-100-raw" 
    }
   ]
  schema_change_policy = ({
    delete_behavior = "DELETE_FROM_DATABASE"
    update_behavior = "UPDATE_IN_DATABASE"
  })
  recrawl_policy = ({
    recrawl_behavior = "CRAWL_EVERYTHING"
  })
  glue_crawler_classifiers = []
 ### glue catalog_db in RAW bucket
  create_glue_catalog_db = false
  create_glue_connection = false
  glue_catalog_db_name   = module.glue_db_raw.glue_catalog_db_name
  glue_catalog_db_description = "Glue catalog db with metadata about ingested data into raw bucket."
  glue_location_uri           = "s3://stg-dlk-${var.env}-${var.data_source}-raw" 
  tags                   = local.common_tags
}

#############################################
### ds100 Glue Job Source to raw zone ###
#############################################

### Helpscout API connection parameters and secret  ## Verify
resource "aws_secretsmanager_secret" "ds100_Helpscout_API_connection_parameters" {
  name   = "/stg-dlk-${var.env}/ds100/Helpscout_connection_parameters"
  kms_key_id = var.external_kms_key_arn
  }

resource "aws_ssm_parameter" "ds100_Helpscout_API_Updated_Timestamp" {
  name   = "/stg-dlk-${var.env}/ds100/helpscout/customers/last_updated"
  type   = "SecureString"
  key_id = var.external_kms_key_arn
  value  = "1970-01-01T00:00:00Z" #yyyy-MM-dd'T'HH:mm:ss'Z'
}


## Glue job source to raw role
module "glue_job_source_to_raw_role" {
  source    = "./dlk-shared-modules/modules/iam"
  role_name = "stg-dlk-${var.env}-ds100-source-to-raw-glue-job-role"
  assume_role_policy = templatefile("${path.module}/templates/assume_role_policy_no_condition.tpl", {
    service = "glue.amazonaws.com"
  })
  policy_name                  = "stg-dlk-${var.env}-ds100-source-to-raw-glue-job-policy"
  policy_file_path             = "${path.module}/artifacts/ds100/glue/policies/glue_job_source_to_raw_policy.json"
  policy_description           = "ds100 Glue Job for source to raw processing"
  tags                         = local.common_tags
  policy_vars = {
    env                        = var.env
    kms_key_arn                = var.external_kms_key_arn
    bucket_artifacts           = var.s3_bucket_code_artifacts_name
    ds_id                      = 100
    account_id                 = local.account_id
    db_name                    = module.glue_db_raw.glue_catalog_db_name
    secret_param               = aws_secretsmanager_secret.ds100_Helpscout_API_connection_parameters.arn ##verify
  }
}


# import glue script to s3
resource "aws_s3_object" "ds100-job-source-to-raw" {
  bucket             = var.s3_bucket_code_artifacts_name
  key                = "artifacts/ds100/glue_job_ds100/source_to_raw_job.py"
  source             = "${path.module}/artifacts/ds100/glue/code/source_to_raw_job.py" 
  force_destroy      = true
  source_hash        = filemd5("${path.module}/artifacts/ds100/glue/code/source_to_raw_job.py")
  bucket_key_enabled = true
  tags               = local.common_tags
}

# Glue Job source to raw  
resource "aws_security_group" "ds100_source_to_raw_glue_job_sg" {
  name        = "stg-dlk-${var.env}-ds-100-network-sg"
  description = "SG for the ds100 glue job"
  vpc_id      = var.vpc_id

  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

# Glue job for passing from source to raw
module "ds100-job-source-to-raw" {
  source                          = "./dlk-shared-modules/modules/glue"
  create_glue_job                 = true
  glue_job_name                   = "stg-dlk-${var.env}-ds100-job-source-to-raw"
  glue_job_role_arn               = module.glue_job_source_to_raw_role.iam_role_arn
  glue_job_description            = "Glue job to ingest Helpscout API data to raw zone"
  glue_job_security_configuration = var.glue_security_configuration_name
  glue_job_default_arguments = {
    "--enable-metrics"                   = false
    "--JOB_NAME"                         = "stg-dlk-${var.env}-ds100-job-source-to-raw"
    "--db_name"                          = "stg-dlk-${var.env}-${var.data_source}-raw-db"
    "--bucket_name"                      = "stg-dlk-${var.env}-ds-100-raw" 
    "--region_name"                      = local.region
    "--s3_Key"                           = "helpscout/Customers/Customers_Info.parquet"
    "--ssm_param_name"                   = aws_ssm_parameter.ds100_Helpscout_API_Updated_Timestamp.name  ##${account_id}:parameter/stg-dlk-${env}/*"
    "--Helpscout_api_url"                = "https://api.helpscout.net/v2/customers"  
    "--helpscout_api_base_url"           = var.helpscout_api_base_url
    "--helpscout_app_id"                 = var.helpscout_app_id
    "--helpscout_app_secret"             = var.helpscout_app_secret
    "--enable-spark-ui"                  = false
    "--enable-job-insights"              = false
    "--enable-glue-datacatalog"          = true
    "--enable-continuous-cloudwatch-log" = true
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${var.s3_bucket_glue_job_temp_name}/temporary/"
    "--env"                              = var.env
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--datalake-formats"                 = "iceberg"
    "--account_id"                       = local.account_id
    "--aws_catalog_name"                 = "AwsDataCatalog"
  }
  glue_version               = "4.0"
  glue_job_number_of_workers = 2
  glue_job_worker_type       = "G.2X"
  glue_job_max_retries       = 0
  glue_job_execution_property = ({
    max_concurrent_runs = 100
  })
  glue_job_command = {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_code_artifacts_name}/${aws_s3_object.ds100-job-source-to-raw.id}" 
    python_version  = 3
  }
  create_glue_catalog_db = false
  create_glue_crawler    = false
  create_glue_connection = true
 
  glue_connector_name  = "stg-dlk-${var.env}-ds100-network-connection"
  glue_connection_type = "NETWORK"
  glue_connection_network = ({
    availability_zone      = var.glue_job_az
    security_group_id_list = [aws_security_group.ds100_source_to_raw_glue_job_sg.id]
    subnet_id              = var.ds100_glue_job_subnet
  })

  ### TAGS
  tags = local.common_tags
}




