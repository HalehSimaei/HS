{
  "Comment": "A description of my state machine",
  "StartAt": "run_glue_job_raw_to_refined",
  "States": {
    "run_glue_job_raw_to_refined": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "${glue_job_name}",
        "Arguments": {}
      },
      "End": true
    }
  }
}