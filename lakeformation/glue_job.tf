# =============================================================================
# AWS Glue Diagnostic Jobs
# =============================================================================

# -----------------------------------------------------------------------------
# Lake Formation FGAC Diagnostics Job
# -----------------------------------------------------------------------------

resource "aws_glue_job" "lf_fgac_diagnostics" {
  name     = "diagnostic-lf"
  role_arn = aws_iam_role.glue_lf_role.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 4 # Minimum required for FGAC

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/diagnostic-lf.py"
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

# -----------------------------------------------------------------------------
# IAM Permissions Diagnostics Job
# -----------------------------------------------------------------------------

resource "aws_glue_job" "iam_diagnostics" {
  name     = "diagnostic-iam"
  role_arn = aws_iam_role.glue_iam_role.arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/diagnostic-iam.py"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-metrics"                 = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                = "true"
    "--spark-event-logs-path"          = "s3://${aws_s3_bucket.glue_logs.bucket}/spark-logs/"
    "--TempDir"                        = "s3://${aws_s3_bucket.glue_temp.bucket}/temp/"
    "--job-language"                   = "python"
    "--job-bookmark-option"            = "job-bookmark-disable"

    # Required parameters
    # "--diagnostic_database" = "your_database"
    # "--diagnostic_table"    = "your_table"

    # Optional parameters
    # "--output_location"      = "s3://your-bucket/diagnostic-reports/"
    # "--test_write_path"      = "s3://your-bucket/diagnostic-write-tests/"
    # "--test_write_database"  = "your_test_database"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  timeout = 60

  tags = {
    Purpose     = "IAM Permissions Diagnostics"
    Environment = var.environment
  }
}

# =============================================================================
# IAM Roles
# =============================================================================

# -----------------------------------------------------------------------------
# Lake Formation Diagnostics Role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "glue_lf_role" {
  name = "glue-diagnostic-lf-role"

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

resource "aws_iam_role_policy_attachment" "glue_lf_service" {
  role       = aws_iam_role.glue_lf_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_lf_lakeformation" {
  name = "lakeformation-data-access"
  role = aws_iam_role.glue_lf_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["lakeformation:GetDataAccess"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_lf_s3" {
  name = "s3-access"
  role = aws_iam_role.glue_lf_role.id

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

resource "aws_iam_role_policy" "glue_lf_glue_api" {
  name = "glue-api-access"
  role = aws_iam_role.glue_lf_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["glue:GetJob"]
        Resource = "*"
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# IAM Diagnostics Role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "glue_iam_role" {
  name = "glue-diagnostic-iam-role"

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
    Purpose = "IAM Permissions Diagnostics"
  }
}

resource "aws_iam_role_policy_attachment" "glue_iam_service" {
  role       = aws_iam_role.glue_iam_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_iam_sts" {
  name = "sts-access"
  role = aws_iam_role.glue_iam_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sts:GetCallerIdentity"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_iam_s3" {
  name = "s3-access"
  role = aws_iam_role.glue_iam_role.id

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

# Additional S3 permissions for write tests (add your data buckets here)
resource "aws_iam_role_policy" "glue_iam_s3_data" {
  count = var.test_write_bucket != null ? 1 : 0

  name = "s3-data-access"
  role = aws_iam_role.glue_iam_role.id

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
          "arn:aws:s3:::${var.test_write_bucket}",
          "arn:aws:s3:::${var.test_write_bucket}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_iam_catalog" {
  name = "glue-catalog-access"
  role = aws_iam_role.glue_iam_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:DeleteTable",
          "glue:UpdateTable"
        ]
        Resource = "*"
      }
    ]
  })
}

# =============================================================================
# S3 Buckets
# =============================================================================

resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.project_name}-glue-scripts-${var.environment}"
}

resource "aws_s3_bucket" "glue_logs" {
  bucket = "${var.project_name}-glue-logs-${var.environment}"
}

resource "aws_s3_bucket" "glue_temp" {
  bucket = "${var.project_name}-glue-temp-${var.environment}"
}

# =============================================================================
# Upload Scripts to S3
# =============================================================================

resource "aws_s3_object" "diagnostic_lf_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/diagnostic-lf.py"
  source = "${path.module}/diagnostic-lf.py"
  etag   = filemd5("${path.module}/diagnostic-lf.py")
}

resource "aws_s3_object" "diagnostic_iam_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/diagnostic-iam.py"
  source = "${path.module}/diagnostic-iam.py"
  etag   = filemd5("${path.module}/diagnostic-iam.py")
}

# =============================================================================
# Lake Formation Permissions (for LF diagnostic only)
# =============================================================================

resource "aws_lakeformation_permissions" "glue_lf_database_access" {
  principal   = aws_iam_role.glue_lf_role.arn
  permissions = ["DESCRIBE"]

  database {
    name = var.diagnostic_database
  }
}

resource "aws_lakeformation_permissions" "glue_lf_table_access" {
  count = var.diagnostic_table != null ? 1 : 0

  principal   = aws_iam_role.glue_lf_role.arn
  permissions = ["SELECT", "DESCRIBE"]

  table {
    database_name = var.diagnostic_database
    name          = var.diagnostic_table
  }
}

# =============================================================================
# Variables
# =============================================================================

variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "glue-diagnostics"
}

variable "diagnostic_database" {
  description = "Database to test diagnostics against"
  type        = string
  default     = "default"
}

variable "diagnostic_table" {
  description = "Optional table to test diagnostics against"
  type        = string
  default     = null
}

variable "test_write_bucket" {
  description = "S3 bucket for IAM diagnostic write tests"
  type        = string
  default     = null
}

# =============================================================================
# Outputs
# =============================================================================

output "lf_diagnostic_job_name" {
  description = "Name of the Lake Formation diagnostic job"
  value       = aws_glue_job.lf_fgac_diagnostics.name
}

output "iam_diagnostic_job_name" {
  description = "Name of the IAM diagnostic job"
  value       = aws_glue_job.iam_diagnostics.name
}

output "lf_role_arn" {
  description = "ARN of the Lake Formation diagnostic IAM role"
  value       = aws_iam_role.glue_lf_role.arn
}

output "iam_role_arn" {
  description = "ARN of the IAM diagnostic IAM role"
  value       = aws_iam_role.glue_iam_role.arn
}
