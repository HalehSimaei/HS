{
  "StartAt": "Generate Date Ranges",
  "States": {
    "Generate Date Ranges": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${lambda_generate_date_ranges}",
        "Payload.$": "$"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "Process Date Ranges"
    },
    "Process Date Ranges": {
      "Type": "Map",
      "MaxConcurrency": 2,
      "ItemsPath": "$.date_ranges",
      "ItemSelector": {
        "shipdate_from.$": "$$.Map.Item.Value.start_date",
        "shipdate_to.$": "$$.Map.Item.Value.end_date"
      },
      "ItemProcessor": {
        "ProcessorConfig": {
          "Mode": "INLINE"
        },
        "StartAt": "RunGlueJobForDateRange",
        "States": {
          "RunGlueJobForDateRange": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
              "JobName": "${glue_job_name}",
              "Arguments": {
                "--shipdate_from.$": "$.shipdate_from",
                "--shipdate_to.$": "$.shipdate_to"
              }
            },
            "End": true
          }
        }
      },
      "ResultPath": null,
      "End": true
    }
  }
}
