# IAM Module - Roles and Policies
# Creates IAM roles and policies for Glue, EMR, and Lambda with least privilege

# Glue Job Execution Role
resource "aws_iam_role" "glue" {
  name = "${var.project_name}-glue-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

# Glue Policy - S3 Access for data lake
resource "aws_iam_role_policy" "glue_s3" {
  name   = "${var.project_name}-glue-s3-policy"
  role   = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3DatalakeAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = concat(
          var.s3_data_lake_bucket_arns,
          [for arn in var.s3_data_lake_bucket_arns : "${arn}/*"]
        )
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = var.cloudwatch_log_group_arns
      }
    ]
  })
}

# EMR Job Execution Role
resource "aws_iam_role" "emr" {
  name = "${var.project_name}-emr-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "elasticmapreduce.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

# EMR Policy - S3 Access
resource "aws_iam_role_policy" "emr_s3" {
  name   = "${var.project_name}-emr-s3-policy"
  role   = aws_iam_role.emr.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "EMRDatalakeAccess"
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ]
      Resource = concat(
        var.s3_data_lake_bucket_arns,
        [for arn in var.s3_data_lake_bucket_arns : "${arn}/*"]
      )
    }]
  })
}

# Lambda Execution Role
resource "aws_iam_role" "lambda" {
  name = "${var.project_name}-lambda-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

# Lambda Policy - Basic execution
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Lambda Policy - S3 Access
resource "aws_iam_role_policy" "lambda_s3" {
  name   = "${var.project_name}-lambda-s3-policy"
  role   = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "LambdaDatalakeAccess"
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject"
      ]
      Resource = [for arn in var.s3_data_lake_bucket_arns : "${arn}/*"]
    }]
  })
}
