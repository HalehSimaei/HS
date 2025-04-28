{
  "StartAt": "Run Glue Job",
  "States": {
    "Run Glue Job": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "${glue_job_name}"
      },
      "Next": "Start Glue Crawler",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "Glue Job Failed"
        }
      ]
    },
    "Start Glue Crawler": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": {
        "Name": "${crawler_name}"
      },
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "Crawler Failed"
        }

      ],
      "Next": "End State" 
    },

    "Glue Job Failed": {
      "Type": "Fail",
      "Error": "GlueJobFailed",
      "Cause": "The Glue job did not succeed."
    },
    "Crawler Failed": {
      "Type": "Fail",
      "Error": "CrawlerFailed",
      "Cause": "The Glue Crawler did not succeed."
    },
    "End State": {
      "Type": "Succeed"
    }
  }
}


