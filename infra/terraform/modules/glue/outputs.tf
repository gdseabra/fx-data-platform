# Glue Module Outputs

output "database_names" {
  value = [
    aws_glue_catalog_database.bronze.name,
    aws_glue_catalog_database.silver.name,
    aws_glue_catalog_database.gold.name,
  ]
}

output "crawler_names" {
  value = [
    aws_glue_crawler.bronze.name,
    aws_glue_crawler.silver.name,
    aws_glue_crawler.gold.name,
  ]
}

output "glue_job_names" {
  value = {
    bronze_ingest    = aws_glue_job.bronze_ingest.name
    silver_transform = aws_glue_job.silver_transform.name
    gold_aggregate   = aws_glue_job.gold_aggregate.name
  }
}
