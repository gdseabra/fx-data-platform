# IAM Module Variables

variable "project_name" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment"
  type        = string
}

variable "s3_data_lake_bucket_arns" {
  description = "ARNs of S3 data lake buckets"
  type        = list(string)
}

variable "cloudwatch_log_group_arns" {
  description = "ARNs of CloudWatch log groups"
  type        = list(string)
}

variable "tags" {
  description = "Tags to apply"
  type        = map(string)
  default     = {}
}
