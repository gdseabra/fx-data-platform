# FX Data Platform - Terraform Configuration
# Main configuration file orchestrating all modules for AWS infrastructure
# This file imports and configures the S3, IAM, and Glue modules

terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  # Uncomment to use S3 backend for state storage
  # backend "s3" {
  #   bucket         = "fx-data-platform-terraform-state"
  #   key            = "prod/terraform.tfstate"
  #   region         = "us-east-1"
  #   encrypt        = true
  #   dynamodb_table = "terraform-lock"
  # }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      CreatedAt   = timestamp()
    }
  }
}

# S3 Module - Data Lake Storage
module "s3" {
  source = "./modules/s3"
  
  project_name = var.project_name
  environment  = var.environment
  aws_region   = var.aws_region
  
  tags = local.common_tags
}

# IAM Module - Roles and Policies
module "iam" {
  source = "./modules/iam"
  
  project_name = var.project_name
  environment  = var.environment
  
  s3_data_lake_bucket_arns = [
    module.s3.raw_bucket_arn,
    module.s3.bronze_bucket_arn,
    module.s3.silver_bucket_arn,
    module.s3.gold_bucket_arn,
    module.s3.ml_bucket_arn,
  ]
  
  cloudwatch_log_group_arns = ["arn:aws:logs:${var.aws_region}:ACCOUNT_ID:*"]
  
  tags = local.common_tags
}

# Glue Module - Data Catalog and Crawlers
module "glue" {
  source = "./modules/glue"
  
  project_name = var.project_name
  environment  = var.environment
  aws_region   = var.aws_region
  
  glue_role_arn = module.iam.glue_role_arn
  
  s3_bronze_bucket  = module.s3.bronze_bucket_name
  s3_silver_bucket  = module.s3.silver_bucket_name
  s3_gold_bucket    = module.s3.gold_bucket_name
  s3_scripts_bucket = module.s3.ml_bucket_name

  tags = local.common_tags
}

# Local variables
locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
