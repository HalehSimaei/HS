{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Deny",
            "Principal": "*",
            "Action": [
                "s3:PutBucketWebsite"
            ],
            "Resource": [
                "arn:aws:s3:::${bucket_name}"
            ]
        },
        {
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Condition": {
                "Bool": {
                    "aws:SecureTransport": false
                }
            },
            "Resource": [
                "arn:aws:s3:::${bucket_name}/*"
            ]
        }
    ]
}