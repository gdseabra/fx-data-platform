# S3 Module Variables

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "enable_encryption" {
  description = "Enable encryption for buckets"
  type        = bool
  default     = true
}

variable "enable_versioning" {
  description = "Enable versioning for buckets"
  type        = bool
  default     = true
}

variable "lifecycle_transition_days" {
  description = "Days before transitioning to Glacier"
  type        = number
  default     = 90
}

variable "tags" {
  description = "Tags to apply"
  type        = map(string)
  default     = {}
}
