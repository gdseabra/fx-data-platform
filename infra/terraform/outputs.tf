# Terraform Outputs for FX Data Platform
# Export important resource identifiers for reference and downstream use

output "s3_raw_bucket_name" {
  description = "Name of the raw (ingestion) S3 bucket"
  value       = module.s3.raw_bucket_name
}

output "s3_bronze_bucket_name" {
  description = "Name of the bronze (cleaned) S3 bucket"
  value       = module.s3.bronze_bucket_name
}

output "s3_silver_bucket_name" {
  description = "Name of the silver (enriched) S3 bucket"
  value       = module.s3.silver_bucket_name
}

output "s3_gold_bucket_name" {
  description = "Name of the gold (aggregated) S3 bucket"
  value       = module.s3.gold_bucket_name
}

output "s3_ml_bucket_name" {
  description = "Name of the ML artifacts S3 bucket"
  value       = module.s3.ml_bucket_name
}

output "glue_job_role_arn" {
  description = "ARN of the Glue job execution role"
  value       = module.iam.glue_role_arn
}

output "emr_job_role_arn" {
  description = "ARN of the EMR job execution role"
  value       = module.iam.emr_role_arn
}

output "lambda_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = module.iam.lambda_role_arn
}

output "glue_database_names" {
  description = "Names of created Glue databases"
  value       = module.glue.database_names
}

output "glue_crawler_names" {
  description = "Names of created Glue crawlers"
  value       = module.glue.crawler_names
}

output "terraform_version" {
  description = "Terraform version used"
  value       = terraform.version
}
