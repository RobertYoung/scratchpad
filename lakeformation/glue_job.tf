# AWS Glue Job for Lake Formation FGAC Diagnostics

resource "aws_glue_job" "lf_fgac_diagnostics" {
  name     = "lf-fgac-diagnostics"
  role_arn = aws_iam_role.glue_job_role.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 4

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/diagnostics.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-lakeformation-fine-grained-access" = "true"
    "--enable-metrics"                           = "true"
    "--enable-continuous-cloudwatch-log"         = "true"
    "--enable-spark-ui"                          = "true"
    "--spark-event-logs-path"                    = "s3://${aws_s3_bucket.glue_logs.bucket}/spark-logs/"
    "--TempDir"                                  = "s3://${aws_s3_bucket.glue_temp.bucket}/temp/"
    "--job-language"                             = "python"
    "--job-bookmark-option"                      = "job-bookmark-disable"

    # Optional diagnostic parameters - uncomment and modify as needed
    # "--diagnostic_database" = "your_database"
    # "--diagnostic_table"    = "your_table"
    # "--output_location"     = "s3://your-bucket/diagnostic-reports/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  timeout = 60

  tags = {
    Purpose     = "Lake Formation FGAC Diagnostics"
    Environment = var.environment
  }
}

# IAM Role for the Glue Job
resource "aws_iam_role" "glue_job_role" {
  name = "lf-fgac-diagnostics-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Purpose = "Lake Formation FGAC Diagnostics"
  }
}

# Attach AWS managed Glue service role policy
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Lake Formation data access policy
resource "aws_iam_role_policy" "lakeformation_access" {
  name = "lakeformation-data-access"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lakeformation:GetDataAccess"
        ]
        Resource = "*"
      }
    ]
  })
}

# S3 access policy for scripts, logs, and temp storage
resource "aws_iam_role_policy" "s3_access" {
  name = "s3-access"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.glue_scripts.arn,
          "${aws_s3_bucket.glue_scripts.arn}/*",
          aws_s3_bucket.glue_logs.arn,
          "${aws_s3_bucket.glue_logs.arn}/*",
          aws_s3_bucket.glue_temp.arn,
          "${aws_s3_bucket.glue_temp.arn}/*"
        ]
      }
    ]
  })
}

# S3 buckets for Glue job assets
resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.project_name}-glue-scripts-${var.environment}"
}

resource "aws_s3_bucket" "glue_logs" {
  bucket = "${var.project_name}-glue-logs-${var.environment}"
}

resource "aws_s3_bucket" "glue_temp" {
  bucket = "${var.project_name}-glue-temp-${var.environment}"
}

# Upload the diagnostics script to S3
resource "aws_s3_object" "diagnostics_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/diagnostics.py"
  source = "${path.module}/diagnostics.py"
  etag   = filemd5("${path.module}/diagnostics.py")
}

# Lake Formation permissions for the Glue role
resource "aws_lakeformation_permissions" "glue_database_access" {
  principal   = aws_iam_role.glue_job_role.arn
  permissions = ["DESCRIBE"]

  database {
    name = var.diagnostic_database
  }
}

resource "aws_lakeformation_permissions" "glue_table_access" {
  count = var.diagnostic_table != null ? 1 : 0

  principal   = aws_iam_role.glue_job_role.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = var.diagnostic_database
    name          = var.diagnostic_table
  }
}

# Variables
variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "lakeformation-diagnostics"
}

variable "diagnostic_database" {
  description = "Database to grant Lake Formation permissions on"
  type        = string
  default     = "default"
}

variable "diagnostic_table" {
  description = "Optional table to grant Lake Formation permissions on"
  type        = string
  default     = null
}

# Outputs
output "glue_job_name" {
  description = "Name of the Glue job"
  value       = aws_glue_job.lf_fgac_diagnostics.name
}

output "glue_role_arn" {
  description = "ARN of the Glue job IAM role"
  value       = aws_iam_role.glue_job_role.arn
}
