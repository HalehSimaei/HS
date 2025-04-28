{
  "Version" : "2012-10-17",
  "Statement" : [
    {
      "Effect" : "Allow",
      "Action" : "sts:AssumeRole",
      "Principal": {
        "Service": "${service}"
      },
      "Condition": {
        "StringEquals": {
          "aws:SourceAccount": "${account_id}"
        }
      }
    }
  ]
}