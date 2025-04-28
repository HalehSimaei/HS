###########
## ds100 ##
###########

## RAW ##

# EventBridge Rule for ds100 Raw Step Function#
resource "aws_cloudwatch_event_rule" "ds100_raw_step_sfn_event_rule" {
  name                = "stg-dlk-${var.env}-ds100-raw-sfn-rule"
  description         = "EventBridge rule for ds100 raw  step function"
  schedule_expression = "cron(43 15 28 4 ? *)" # Run at 04:00 UTC daily, Monday to Friday
}

#IAM Role for EventBridge Rule to invoke raw Step Function#
module "ds100_eventbridge_raw_sfn_role" {
  source = "./dlk-shared-modules/modules/iam"

  role_name = "stg-dlk-${var.env}-ds100-eventbridge-sfn-role"
  assume_role_policy = templatefile("${path.module}/templates/assume_role_policy_no_condition.tpl", {
    service = "events.amazonaws.com"
  })
  policy_name        = "stg-dlk-${var.env}-eventbridge-ds100-invoke-sfn-policy"
  policy_file_path   = "templates/event_bridge_policy.tpl"
  policy_description = "ds100 EventBridge Invoke Source to Raw Step Function Policy"
  tags               = local.common_tags
  policy_vars = {
    resource_arn = module.ds100_raw_sfn.state_machine_arn
    source_arn   = aws_cloudwatch_event_rule.ds100_raw_step_sfn_event_rule.name
  }
}

#EventBridge Rule Target to invoke raw Step Function#
resource "aws_cloudwatch_event_target" "ds100_raw_step_sfn_event_target" {
  rule      = aws_cloudwatch_event_rule.ds100_raw_step_sfn_event_rule.name
  target_id = "scheduled-invoke-ds100-raw-step-function"
  arn       = module.ds100_raw_sfn.state_machine_arn
  role_arn  = module.ds100_eventbridge_raw_sfn_role.iam_role_arn
}

## REFINED ##



## CURATED ##
