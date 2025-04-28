{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogDelivery",
                "logs:CreateLogStream",
                "logs:GetLogDelivery",
                "logs:UpdateLogDelivery",
                "logs:DeleteLogDelivery",
                "logs:ListLogDeliveries",
                "logs:PutLogEvents",
                "logs:PutResourcePolicy",
                "logs:DescribeResourcePolicies",
                "logs:DescribeLogGroups"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns",
                "glue:BatchStopJobRun"
            ],
            "Resource": [
                "${glue_job_arn}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction",
                "lambda:GetFunction"
            ],
            "Resource": [
                "arn:aws:lambda:${region}:${account_id}:function:${lambda_name}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:StartCrawler",
                "glue:GetCrawler",
                "glue:GetCrawlerMetrics",
                "glue:StopCrawler"
            ],
            "Resource": [
                "arn:aws:glue:${region}:${account_id}:crawler/${crawler_name}"
            ]
        }
    ]
}
