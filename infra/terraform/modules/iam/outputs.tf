# IAM Module Outputs

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "emr_role_arn" {
  value = aws_iam_role.emr.arn
}

output "lambda_role_arn" {
  value = aws_iam_role.lambda.arn
}
