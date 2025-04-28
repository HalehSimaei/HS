##########################
##########################
### DS100 LF PERMISSIONS ###
##########################
#########################


##############
## RAW PART ##
##############
# This section contains the Lake Formation permissions for the ds100 raw data source.
# 1. Register S3 bucket
resource "aws_lakeformation_resource" "ds100_raw_data_location" {
  arn      = data.aws_s3_bucket.ds100-raw-s3-bucket.arn
  role_arn = module.ds100_lf_role_raw.iam_role_arn
}

# 2. Glue ETL Job - Data location access
resource "aws_lakeformation_permissions" "ds100_raw_data_location_access" {
  principal   = module.ds100_lf_role_raw.iam_role_arn
  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn         = data.aws_s3_bucket.ds100-raw-s3-bucket.arn
    catalog_id  = local.account_id
  }

  depends_on = [aws_lakeformation_resource.ds100_raw_data_location]
}

# 3. Glue ETL Job - Database permissions
resource "aws_lakeformation_permissions" "ds100_raw_datacatalog_lf_access" {
  principal   = module.ds100_lf_role_raw.iam_role_arn
  permissions = ["CREATE_TABLE", "ALTER", "DESCRIBE"]

  database {
    name       = module.glue_db_raw.glue_catalog_db_name
    catalog_id = local.account_id
  }
}

# 4. Glue ETL Job - Table permissions
resource "aws_lakeformation_permissions" "ds100_raw_datacatalog_lf_table_access" {
  principal   = module.ds100_lf_role_raw.iam_role_arn
  permissions = ["ALL"]

  table {
    database_name = module.glue_db_raw.glue_catalog_db_name
    name          = "helpscout_customers"
    catalog_id    = local.account_id
  }
}

# 5. Crawler - Data location access
resource "aws_lakeformation_permissions" "ds100_raw_crawler_location_access" {
  principal   = module.glue_ds100_crawler_role_raw.iam_role_arn
  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn         = data.aws_s3_bucket.ds100-raw-s3-bucket.arn
    catalog_id  = local.account_id
  }

  depends_on = [aws_lakeformation_resource.ds100_raw_data_location]
}

# 6. Crawler - Database permissions
resource "aws_lakeformation_permissions" "ds100_raw_crawler_db_access" {
  principal   = module.glue_ds100_crawler_role_raw.iam_role_arn
  permissions = ["CREATE_TABLE", "ALTER", "DESCRIBE"]

  database {
    name       = module.glue_db_raw.glue_catalog_db_name
    catalog_id = local.account_id
  }
}

# 7. Crawler - Table wildcard permissions
resource "aws_lakeformation_permissions" "ds100_raw_crawler_table_access" {
  principal   = module.glue_ds100_crawler_role_raw.iam_role_arn
  permissions = ["ALL"]

  table {
    database_name = module.glue_db_raw.glue_catalog_db_name
    wildcard      = true
    catalog_id    = local.account_id
  }
}

# 8. Glue Job Role for Source-to-Raw copy - Database access
resource "aws_lakeformation_permissions" "ds100_raw_glue_db_access" {
  principal   = module.glue_job_source_to_raw_role.iam_role_arn
  permissions = ["CREATE_TABLE", "DESCRIBE"]

  database {
    name       = module.glue_db_raw.glue_catalog_db_name
    catalog_id = local.account_id
  }
}

# 9. Glue Job Role for Source-to-Raw copy - Table access
resource "aws_lakeformation_permissions" "ds100_raw_glue_table_access" {
  principal   = module.glue_job_source_to_raw_role.iam_role_arn
  permissions = ["ALL"]

  table {
    database_name = module.glue_db_raw.glue_catalog_db_name
    wildcard      = true
    catalog_id    = local.account_id
  }
}

# S3 bucket data block 
data "aws_s3_bucket" "ds100-raw-s3-bucket" {
  bucket = "stg-dlk-${var.env}-${var.data_source}-raw"
}



#######################
#### REFINED PART #####
#######################




######################
#### CURATED PART ####
######################


