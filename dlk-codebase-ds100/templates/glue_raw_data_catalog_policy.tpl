{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*Object*"
            ],
            "Resource": [
                "arn:aws:s3:::stg-dlk-${env}-ds-${ds_id}-raw/*",
                "arn:aws:s3:::stg-dlk-${env}-wheels/*",
                "arn:aws:s3:::${bucket_artifacts}/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::stg-dlk-${env}-ds-${ds_id}-raw",
                "arn:aws:s3:::${bucket_artifacts}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lakeformation:GetDataAccess",
                "glue:GetSecurityConfiguration",
                "lakeformation:AddLFTagsToResource",
                "lakeformation:CreateDataCellsFilter",
                "lakeformation:GrantPermissions"
            ],
            "Resource": [
                "*"
          ]
        },
        {
            "Action": [
                "kms:Describe*",
                "kms:Get*",
                "kms:List*",
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:GenerateDataKey"
            ],
            "Effect": "Allow",
            "Resource": "${kms_key_arn}"
        },
        {
            "Action": [
                "logs:*"
            ],
            "Effect": "Allow",
            "Resource": "arn:aws:logs:${region_name}:${account_id}:log-group:/aws-glue/jobs/*"
        },
        {
            "Action": [
                "glue:GetTables",
                "glue:GetTable",
                "glue:CreateDatabase",
                "glue:GetDatabase",
                "glue:GetPartitions",
                "glue:CreateTable",
                "glue:DeleteTable",
                "glue:BatchCreatePartition"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:glue:${region_name}:${account_id}:database/stg-dlk-${env}-ds-${ds_id}-raw-db",
                "arn:aws:glue:${region_name}:${account_id}:database/${raw_db_name}",
                "arn:aws:glue:${region_name}:${account_id}:catalog",
                "arn:aws:glue:${region_name}:${account_id}:table/stg-dlk-${env}-ds-${ds_id}-raw-db/*",
                "arn:aws:glue:${region_name}:${account_id}:table/${raw_db_name}/*"
            ]
        },
        {
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults"
            ],
            "Effect": "Allow",
            "Resource": "arn:aws:athena:${region_name}:${account_id}:workgroup/primary"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::stg-dlk-${env}-athena-query-results",
                "arn:aws:s3:::stg-dlk-${env}-athena-query-results/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::stg-dlk-${env}-athena-query-results"
        }
    ]
}
