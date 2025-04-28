############################
### ds100 LF Permissions ###
############################


### Add LF-Tags to Databases - RAW
resource "aws_lakeformation_resource_lf_tags" "tf_tags_ds100_database_raw" {
  database {
    name = module.glue_db_raw.glue_catalog_db_name
  }
  lf_tag {
    key   = "data_contents_patient"
    value = "false"
  }
  lf_tag {
    key   = "data_contents_production"
    value = "false"
  }
  lf_tag {
    key   = "data_contents_personal"
    value = "true"
  }
  lf_tag {
    key   = "data_confidentialitylevel"
    value = "strictly_confidential"
  }
  lf_tag {
    key   = "data_zone"
    value = "raw"
  }
  lf_tag {
    key   = "sales"
    value = "false"
  }
  lf_tag {
    key   = "complaints"
    value = "false"
  }
  lf_tag {
    key   = "domain"
    value = "No_Domain"
  }

  depends_on = [module.glue_db_raw.glue_catalog_db_name]
  
}


### Add LF-Tags to Databases - REFINED



### Add LF-Tags to Databases - CURATED
