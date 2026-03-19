# S3 Module Outputs

output "raw_bucket_name" {
  value = aws_s3_bucket.raw.id
}

output "raw_bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.id
}

output "bronze_bucket_arn" {
  value = aws_s3_bucket.bronze.arn
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.id
}

output "silver_bucket_arn" {
  value = aws_s3_bucket.silver.arn
}

output "gold_bucket_name" {
  value = aws_s3_bucket.gold.id
}

output "gold_bucket_arn" {
  value = aws_s3_bucket.gold.arn
}

output "ml_bucket_name" {
  value = aws_s3_bucket.ml.id
}

output "ml_bucket_arn" {
  value = aws_s3_bucket.ml.arn
}
