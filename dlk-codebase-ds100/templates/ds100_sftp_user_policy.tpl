{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*Object*"
            ],
            "Resource": [
                "arn:aws:s3:::stg-dlk-${env}-ds100-raw/ds100_sftp_user/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::stg-dlk-${env}-ds100-raw"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "kms:Describe*",
                "kms:Get*",
                "kms:List*",
                "kms:Encrypt",
                "kms:Decrypt",
                "kms:GenerateDataKey"
            ],
            "Resource": "${kms_arn}"
        }
    ]
}