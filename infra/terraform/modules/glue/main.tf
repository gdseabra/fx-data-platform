# Glue Module - Data Catalog and Crawlers
# Creates Glue databases and crawlers for the data lake

resource "aws_glue_catalog_database" "bronze" {
  name = "${var.project_name}_bronze"

  description = "Bronze layer: deduplicated and cleaned raw data"
}

resource "aws_glue_catalog_database" "silver" {
  name = "${var.project_name}_silver"

  description = "Silver layer: transformed and enriched data"
}

resource "aws_glue_catalog_database" "gold" {
  name = "${var.project_name}_gold"

  description = "Gold layer: aggregated analytics-ready data"
}

# Glue Crawler for Bronze Layer
resource "aws_glue_crawler" "bronze" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "${var.project_name}-bronze-crawler-${var.environment}"
  role_arn      = var.glue_role_arn

  s3_target {
    path = "s3://${var.s3_bronze_bucket}/"
  }

  table_prefix = "tbl_"

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })

  schedule_expression = "cron(0 7 * * ? *)"  # Daily at 7 AM UTC

  tags = {
    Layer = "Bronze"
  }
}

# Glue Crawler for Silver Layer
resource "aws_glue_crawler" "silver" {
  database_name = aws_glue_catalog_database.silver.name
  name          = "${var.project_name}-silver-crawler-${var.environment}"
  role_arn      = var.glue_role_arn

  s3_target {
    path = "s3://${var.s3_silver_bucket}/"
  }

  table_prefix = "tbl_"

  schedule_expression = "cron(0 9 * * ? *)"  # Daily at 9 AM UTC

  tags = {
    Layer = "Silver"
  }
}

# Glue Crawler for Gold Layer
resource "aws_glue_crawler" "gold" {
  database_name = aws_glue_catalog_database.gold.name
  name          = "${var.project_name}-gold-crawler-${var.environment}"
  role_arn      = var.glue_role_arn

  s3_target {
    path = "s3://${var.s3_gold_bucket}/"
  }

  table_prefix = "tbl_"

  schedule_expression = "cron(0 11 * * ? *)"  # Daily at 11 AM UTC

  tags = {
    Layer = "Gold"
  }
}

# ---------------------------------------------------------------------------
# Glue Jobs — PySpark ETL scripts for each lakehouse layer
# ---------------------------------------------------------------------------

resource "aws_glue_job" "bronze_ingest" {
  name     = "${var.project_name}-bronze-ingest-${var.environment}"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_scripts_bucket}/glue-scripts/bronze/ingest_transactions.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${var.s3_scripts_bucket}/glue-tmp/"
    "--extra-py-files"                   = "s3://${var.s3_scripts_bucket}/glue-scripts/common.zip"
    "--ENV"                              = var.environment
    "--BRONZE_BUCKET"                    = var.s3_bronze_bucket
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60  # minutes

  tags = merge(var.tags, { Layer = "Bronze" })
}

resource "aws_glue_job" "silver_transform" {
  name     = "${var.project_name}-silver-transform-${var.environment}"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_scripts_bucket}/glue-scripts/silver/transform_transactions.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${var.s3_scripts_bucket}/glue-tmp/"
    "--extra-py-files"                   = "s3://${var.s3_scripts_bucket}/glue-scripts/common.zip"
    "--ENV"                              = var.environment
    "--BRONZE_BUCKET"                    = var.s3_bronze_bucket
    "--SILVER_BUCKET"                    = var.s3_silver_bucket
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  tags = merge(var.tags, { Layer = "Silver" })
}

resource "aws_glue_job" "gold_aggregate" {
  name     = "${var.project_name}-gold-aggregate-${var.environment}"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_scripts_bucket}/glue-scripts/gold/aggregate_metrics.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = "s3://${var.s3_scripts_bucket}/glue-tmp/"
    "--extra-py-files"                   = "s3://${var.s3_scripts_bucket}/glue-scripts/common.zip"
    "--ENV"                              = var.environment
    "--SILVER_BUCKET"                    = var.s3_silver_bucket
    "--GOLD_BUCKET"                      = var.s3_gold_bucket
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  tags = merge(var.tags, { Layer = "Gold" })
}
