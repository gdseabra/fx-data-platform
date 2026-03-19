# Terraform variable values for dev environment
# Usage: terraform apply -var-file=environments/dev/terraform.tfvars

aws_region                = "us-east-1"
project_name              = "fx-data-platform"
environment               = "dev"
enable_versioning         = true
lifecycle_transition_days = 90
