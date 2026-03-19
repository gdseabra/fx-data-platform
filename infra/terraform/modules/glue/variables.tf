# Glue Module Variables

variable "project_name" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "glue_role_arn" {
  description = "ARN of Glue execution role"
  type        = string
}

variable "s3_bronze_bucket" {
  description = "Bronze bucket name"
  type        = string
}

variable "s3_silver_bucket" {
  description = "Silver bucket name"
  type        = string
}

variable "s3_gold_bucket" {
  description = "Gold bucket name"
  type        = string
}

variable "s3_scripts_bucket" {
  description = "S3 bucket name where PySpark job scripts are stored"
  type        = string
}

variable "tags" {
  description = "Tags to apply"
  type        = map(string)
  default     = {}
}
