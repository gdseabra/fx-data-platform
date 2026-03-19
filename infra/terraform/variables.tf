# Terraform Variables for FX Data Platform
# Define all input variables used across modules

variable "aws_region" {
  description = "AWS region for resource deployment"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming and tagging"
  type        = string
  default     = "fx-data-platform"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

variable "enable_encryption" {
  description = "Enable encryption for S3 buckets and databases"
  type        = bool
  default     = true
}

variable "lifecycle_transition_days" {
  description = "Days before transitioning objects to Glacier (raw layer)"
  type        = number
  default     = 90
}

variable "mfa_delete_enabled" {
  description = "Enable MFA delete protection on S3 buckets"
  type        = bool
  default     = false
}

variable "cors_allowed_origins" {
  description = "CORS allowed origins for S3 buckets"
  type        = list(string)
  default     = ["*"]
}

variable "enable_versioning" {
  description = "Enable versioning for S3 buckets"
  type        = bool
  default     = true
}

variable "glue_database_names" {
  description = "Names of Glue databases to create"
  type        = list(string)
  default     = ["fx_bronze", "fx_silver", "fx_gold"]
}

variable "enable_cloudtrail_logging" {
  description = "Enable CloudTrail logging for audit trail"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}
