# S3 Bucket Module - Data Lake Storage
# Creates and configures S3 buckets for the FX Data Platform data lake
# Includes: versioning, encryption, lifecycle policies, and access controls

resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-datalake-raw-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  
  tags = merge(
    var.tags,
    {
      Layer       = "raw"
      Description = "Raw ingestion data from streaming sources"
    }
  )
}

resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-datalake-bronze-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  
  tags = merge(
    var.tags,
    {
      Layer       = "bronze"
      Description = "Cleaned and deduplicated data"
    }
  )
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-datalake-silver-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  
  tags = merge(
    var.tags,
    {
      Layer       = "silver"
      Description = "Transformed and enriched data"
    }
  )
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-datalake-gold-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  
  tags = merge(
    var.tags,
    {
      Layer       = "gold"
      Description = "Aggregated data ready for analytics"
    }
  )
}

resource "aws_s3_bucket" "ml" {
  bucket = "${var.project_name}-datalake-ml-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  
  tags = merge(
    var.tags,
    {
      Layer       = "ml"
      Description = "ML models, features, and artifacts"
    }
  )
}

# Versioning for all buckets
resource "aws_s3_bucket_versioning" "all" {
  for_each = toset([
    aws_s3_bucket.raw.id,
    aws_s3_bucket.bronze.id,
    aws_s3_bucket.silver.id,
    aws_s3_bucket.gold.id,
    aws_s3_bucket.ml.id,
  ])

  bucket = each.value

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

# Encryption for all buckets
resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = toset([
    aws_s3_bucket.raw.id,
    aws_s3_bucket.bronze.id,
    aws_s3_bucket.silver.id,
    aws_s3_bucket.gold.id,
    aws_s3_bucket.ml.id,
  ])

  bucket = each.value

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "all" {
  for_each = toset([
    aws_s3_bucket.raw.id,
    aws_s3_bucket.bronze.id,
    aws_s3_bucket.silver.id,
    aws_s3_bucket.gold.id,
    aws_s3_bucket.ml.id,
  ])

  bucket = each.value

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle policies for raw bucket (move to Glacier after 90 days)
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "transition-to-glacier"
    status = "Enabled"

    transition {
      days          = var.lifecycle_transition_days
      storage_class = "GLACIER"
    }

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 365
    }
  }
}

# Data source to get current AWS account ID
data "aws_caller_identity" "current" {}

# Data source to get region
data "aws_region" "current" {}
