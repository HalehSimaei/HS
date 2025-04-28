{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "logs:CreateLogGroup",
      "Resource": "arn:aws:logs:${region}:${account_id}:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:${region}:${account_id}:log-group:/aws/lambda/${lambda_name}:*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction",
        "lambda:GetFunctionConfiguration",
        "lambda:ListFunctions"
      ],
      "Resource": "arn:aws:lambda:${region}:${account_id}:function:${lambda_name}"
    }
  ]
}
