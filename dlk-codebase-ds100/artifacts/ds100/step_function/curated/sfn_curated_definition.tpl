{
  "Comment": "A description of my state machine",
  "StartAt": "run_glue_job_raw_to_refine",
  "States": {
    "run_glue_job_raw_to_refine": {
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