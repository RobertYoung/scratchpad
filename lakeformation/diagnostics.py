"""
AWS Glue ETL Job: Lake Formation Fine-Grained Access Control (FGAC) Diagnostics
================================================================================

This job performs comprehensive diagnostic checks to verify Lake Formation FGAC
permissions are working correctly in AWS Glue 5.0+.

Based on AWS documentation limitations:
https://docs.aws.amazon.com/glue/latest/dg/security-lf-enable.html
https://docs.aws.amazon.com/glue/latest/dg/security-lf-enable-considerations.html

Usage:
------
Deploy as an AWS Glue job with the following parameters:
  --enable-lakeformation-fine-grained-access: true
  --number-of-workers: 4 (minimum required)
  --additional-python-modules: (none required)

Optional job parameters:
  --diagnostic_database: Database name to test (default: "default")
  --diagnostic_table: Table name to test (default: None - will list available)
  --output_location: S3 path for diagnostic report output
"""

import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

# AWS Glue imports
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# PySpark imports
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LF-FGAC-Diagnostics")


class DiagnosticStatus(Enum):
    """Status codes for diagnostic checks."""
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARNING"
    SKIP = "SKIPPED"
    INFO = "INFO"


@dataclass
class DiagnosticResult:
    """Container for individual diagnostic check results."""
    check_name: str
    status: str
    message: str
    details: Optional[Dict[str, Any]] = None
    recommendation: Optional[str] = None


@dataclass
class DiagnosticReport:
    """Container for full diagnostic report."""
    job_name: str
    run_id: str
    timestamp: str
    glue_version: str
    total_checks: int
    passed: int
    failed: int
    warnings: int
    results: List[Dict[str, Any]]


class LakeFormationFGACDiagnostics:
    """
    Diagnostic utility for AWS Glue + Lake Formation FGAC integration.
    
    Checks for common configuration issues and validates that fine-grained
    access controls are properly configured and functioning.
    """
    
    # Regions where Lake Formation + Glue is NOT supported
    UNSUPPORTED_REGIONS = ["us-gov-east-1", "us-gov-west-1"]
    
    # Supported table formats for FGAC
    SUPPORTED_FORMATS = ["hive", "iceberg"]
    SUPPORTED_HIVE_FORMATS = ["parquet", "orc", "csv"]
    
    # Iceberg metadata tables that are supported with FGAC
    SUPPORTED_ICEBERG_METADATA_TABLES = [
        "history", "metadata_log_entries", "snapshots", 
        "files", "manifests", "refs"
    ]
    
    # Hidden columns in Iceberg metadata (sensitive data)
    HIDDEN_ICEBERG_COLUMNS = ["partitions", "path", "summaries"]
    
    # Minimum workers required for FGAC
    MIN_WORKERS_FGAC = 4
    
    def __init__(self, spark: SparkSession, glue_context: GlueContext, args: Dict[str, str]):
        """Initialize the diagnostics utility."""
        self.spark = spark
        self.glue_context = glue_context
        self.args = args
        self.results: List[DiagnosticResult] = []
        self.job_name = args.get("JOB_NAME", "unknown")
        self.run_id = args.get("JOB_RUN_ID", "unknown")
        
    def add_result(self, check_name: str, status: DiagnosticStatus, 
                   message: str, details: Dict = None, recommendation: str = None):
        """Add a diagnostic result."""
        result = DiagnosticResult(
            check_name=check_name,
            status=status.value,
            message=message,
            details=details,
            recommendation=recommendation
        )
        self.results.append(result)
        
        # Log the result
        log_msg = f"[{status.value}] {check_name}: {message}"
        if status == DiagnosticStatus.FAIL:
            logger.error(log_msg)
        elif status == DiagnosticStatus.WARN:
            logger.warning(log_msg)
        else:
            logger.info(log_msg)
            
    def check_glue_version(self) -> bool:
        """Verify AWS Glue version is 5.0 or higher (required for FGAC)."""
        try:
            # Get Glue version from Spark config
            glue_version = self.spark.conf.get("spark.glue.version", "unknown")
            
            # Parse version
            if glue_version == "unknown":
                # Try alternative method
                glue_version = self.args.get("--glue_version", "5.0")
            
            major_version = float(glue_version.split(".")[0])
            
            if major_version >= 5:
                self.add_result(
                    "Glue Version Check",
                    DiagnosticStatus.PASS,
                    f"AWS Glue version {glue_version} supports native FGAC",
                    {"glue_version": glue_version}
                )
                return True
            else:
                self.add_result(
                    "Glue Version Check",
                    DiagnosticStatus.FAIL,
                    f"AWS Glue version {glue_version} requires migration for FGAC",
                    {"glue_version": glue_version},
                    "Upgrade to AWS Glue 5.0+ and migrate from GlueContext/DynamicFrame to Spark DataFrame"
                )
                return False
                
        except Exception as e:
            self.add_result(
                "Glue Version Check",
                DiagnosticStatus.WARN,
                f"Could not determine Glue version: {str(e)}",
                recommendation="Ensure job is running on AWS Glue 5.0+"
            )
            return True
            
    def check_fgac_enabled(self) -> bool:
        """
        Verify FGAC is enabled by checking Spark configurations.

        When FGAC is enabled, AWS Glue sets specific Spark configurations
        for Lake Formation integration.
        """
        fgac_indicators = {}
        fgac_enabled = False

        try:
            # Check for Lake Formation specific Spark configurations
            lf_configs_to_check = [
                "spark.glue.lakeformation.enabled",
                "spark.hadoop.fs.s3.authorization.enabled",
                "spark.sql.catalog.spark_catalog.lakeformation.enabled",
            ]

            for config in lf_configs_to_check:
                try:
                    value = self.spark.conf.get(config)
                    fgac_indicators[config] = value
                    if value.lower() == "true":
                        fgac_enabled = True
                except Exception:
                    fgac_indicators[config] = "not_set"

            # Check for Lake Formation authorization class
            try:
                auth_class = self.spark.conf.get("spark.hadoop.fs.s3.authorization.class", "not_set")
                fgac_indicators["authorization_class"] = auth_class
                if "lakeformation" in auth_class.lower():
                    fgac_enabled = True
            except Exception:
                pass

            # Check if running in secure mode (FGAC creates a sandboxed environment)
            try:
                security_manager = self.spark._jsc.sc().env().securityManager()
                if security_manager is not None:
                    fgac_indicators["security_manager_active"] = True
                    fgac_enabled = True
            except Exception:
                fgac_indicators["security_manager_active"] = "unknown"

        except Exception as e:
            fgac_indicators["error"] = str(e)

        if fgac_enabled:
            self.add_result(
                "FGAC Parameter Check",
                DiagnosticStatus.PASS,
                "Lake Formation FGAC is enabled",
                {"indicators": fgac_indicators}
            )
            return True
        else:
            self.add_result(
                "FGAC Parameter Check",
                DiagnosticStatus.FAIL,
                "Lake Formation FGAC does not appear to be enabled",
                {"indicators": fgac_indicators},
                "Add job parameter: --enable-lakeformation-fine-grained-access=true"
            )
            return False
            
    def check_worker_allocation(self) -> bool:
        """
        Verify minimum worker count for FGAC.

        FGAC requires minimum 4 workers:
        - 1 user driver
        - 1 system driver
        - 1 system executor
        - 1 standby user executor
        """
        num_workers = None
        detection_method = None

        try:
            # Method 1: Query the Glue API for job configuration
            try:
                import boto3
                glue_client = boto3.client("glue")
                job_name = self.args.get("JOB_NAME")
                if job_name:
                    job_response = glue_client.get_job(JobName=job_name)
                    num_workers = job_response["Job"].get("NumberOfWorkers")
                    if num_workers:
                        detection_method = "glue_api"
            except Exception:
                pass

            # Method 2: Check Spark executor configuration
            if num_workers is None:
                try:
                    # Get number of executor instances from Spark config
                    executor_instances = self.spark.conf.get("spark.executor.instances", None)
                    if executor_instances:
                        # In Glue, workers = executors + 1 (driver)
                        num_workers = int(executor_instances) + 1
                        detection_method = "spark_executor_instances"
                except Exception:
                    pass

            # Method 3: Count active executors from SparkContext
            if num_workers is None:
                try:
                    sc = self.spark.sparkContext
                    # Get executor count from status tracker
                    executor_ids = sc._jsc.sc().getExecutorIds()
                    num_executors = executor_ids.size()
                    if num_executors > 0:
                        # Add 1 for driver
                        num_workers = num_executors + 1
                        detection_method = "executor_count"
                except Exception:
                    pass

            # Method 4: Check default parallelism as a rough estimate
            if num_workers is None:
                try:
                    parallelism = int(self.spark.conf.get("spark.default.parallelism", "0"))
                    if parallelism > 0:
                        # Rough estimate: parallelism / cores per executor
                        cores_per_executor = int(self.spark.conf.get("spark.executor.cores", "4"))
                        num_workers = max(4, parallelism // cores_per_executor)
                        detection_method = "parallelism_estimate"
                except Exception:
                    pass

            if num_workers is None:
                self.add_result(
                    "Worker Allocation Check",
                    DiagnosticStatus.WARN,
                    "Could not determine worker count",
                    {"detection_methods_tried": ["glue_api", "spark_executor_instances", "executor_count", "parallelism_estimate"]},
                    "Ensure job is configured with at least 4 workers for FGAC"
                )
                return True

            worker_allocation = {
                "total_workers": num_workers,
                "detection_method": detection_method,
                "user_driver": 1,
                "system_driver": 1,
                "minimum_required": self.MIN_WORKERS_FGAC
            }

            if num_workers >= 4:
                # Calculate executor allocation (10% reserved for user executors)
                remaining = num_workers - 2  # Subtract drivers
                user_executor_reserve = max(1, int(remaining * 0.1))
                system_executors = remaining - user_executor_reserve

                worker_allocation.update({
                    "user_executor_reserve": user_executor_reserve,
                    "system_executors_available": system_executors
                })

                self.add_result(
                    "Worker Allocation Check",
                    DiagnosticStatus.PASS,
                    f"Worker count ({num_workers}) meets FGAC requirements",
                    worker_allocation
                )
                return True
            else:
                self.add_result(
                    "Worker Allocation Check",
                    DiagnosticStatus.FAIL,
                    f"Worker count ({num_workers}) is below minimum ({self.MIN_WORKERS_FGAC}) for FGAC",
                    worker_allocation,
                    f"Increase --number-of-workers to at least {self.MIN_WORKERS_FGAC}"
                )
                return False

        except Exception as e:
            self.add_result(
                "Worker Allocation Check",
                DiagnosticStatus.WARN,
                f"Could not verify worker allocation: {str(e)}"
            )
            return True
            
    def check_region_support(self) -> bool:
        """Verify the job is running in a supported region."""
        try:
            # Get region from Spark config or environment
            region = self.spark.conf.get("spark.hadoop.aws.region", None)
            
            if region is None:
                # Try alternative sources
                import boto3
                session = boto3.Session()
                region = session.region_name
                
            if region in self.UNSUPPORTED_REGIONS:
                self.add_result(
                    "Region Support Check",
                    DiagnosticStatus.FAIL,
                    f"Region {region} does not support Glue + Lake Formation",
                    {"region": region, "unsupported_regions": self.UNSUPPORTED_REGIONS},
                    "Use a supported AWS region"
                )
                return False
            else:
                self.add_result(
                    "Region Support Check",
                    DiagnosticStatus.PASS,
                    f"Region {region} supports Glue + Lake Formation",
                    {"region": region}
                )
                return True
                
        except Exception as e:
            self.add_result(
                "Region Support Check",
                DiagnosticStatus.WARN,
                f"Could not determine region: {str(e)}"
            )
            return True
            
    def check_iam_permissions(self) -> bool:
        """Verify required IAM permissions are in place."""
        required_permissions = {
            "glue:Get*": False,
            "lakeformation:GetDataAccess": False
        }

        try:
            # Test Glue catalog access using SQL (spark.catalog.* uses RDDs which are blocked by FGAC)
            databases_df = self.spark.sql("SHOW DATABASES")
            db_list = [row["databaseName"] for row in databases_df.collect()]
            required_permissions["glue:Get*"] = True

            self.add_result(
                "IAM Glue Permissions Check",
                DiagnosticStatus.PASS,
                f"Can access Glue Catalog ({len(db_list)} databases found)",
                {"databases": db_list[:10]}  # Limit to first 10
            )

        except Exception as e:
            self.add_result(
                "IAM Glue Permissions Check",
                DiagnosticStatus.FAIL,
                f"Cannot access Glue Catalog: {str(e)}",
                recommendation="Ensure IAM role has glue:Get* permissions"
            )
            return False

        # Note: lakeformation:GetDataAccess is tested implicitly when reading data
        self.add_result(
            "IAM Lake Formation Permissions",
            DiagnosticStatus.INFO,
            "Lake Formation permissions will be tested during data access",
            recommendation="Ensure IAM role has lakeformation:GetDataAccess permission"
        )

        return True
        
    def check_table_format(self, database: str, table: str) -> Tuple[bool, str]:
        """
        Verify table format is supported for FGAC.
        
        Supported: Hive (Parquet, ORC, CSV) and Iceberg
        """
        try:
            # Get table metadata
            table_metadata = self.spark.sql(f"DESCRIBE EXTENDED {database}.{table}")
            metadata_dict = {row["col_name"]: row["data_type"] 
                           for row in table_metadata.collect() 
                           if row["col_name"]}
            
            # Check for Iceberg
            provider = metadata_dict.get("Provider", "").lower()
            table_type = metadata_dict.get("Type", "").lower()
            serde = metadata_dict.get("Serde Library", "").lower()
            
            format_info = {
                "database": database,
                "table": table,
                "provider": provider,
                "type": table_type
            }
            
            if "iceberg" in provider or "iceberg" in str(metadata_dict):
                self.add_result(
                    "Table Format Check",
                    DiagnosticStatus.PASS,
                    f"Table {database}.{table} is Iceberg format (supported)",
                    format_info
                )
                return True, "iceberg"
                
            elif any(fmt in serde.lower() for fmt in ["parquet", "orc"]) or \
                 any(fmt in str(metadata_dict).lower() for fmt in self.SUPPORTED_HIVE_FORMATS):
                detected_format = next(
                    (fmt for fmt in self.SUPPORTED_HIVE_FORMATS 
                     if fmt in str(metadata_dict).lower()), 
                    "hive"
                )
                format_info["format"] = detected_format
                self.add_result(
                    "Table Format Check",
                    DiagnosticStatus.PASS,
                    f"Table {database}.{table} is Hive/{detected_format} format (supported)",
                    format_info
                )
                return True, "hive"
                
            else:
                self.add_result(
                    "Table Format Check",
                    DiagnosticStatus.WARN,
                    f"Could not confirm table format for {database}.{table}",
                    format_info,
                    "FGAC only supports Hive (Parquet, ORC, CSV) and Iceberg tables"
                )
                return True, "unknown"
                
        except Exception as e:
            self.add_result(
                "Table Format Check",
                DiagnosticStatus.FAIL,
                f"Error checking table format: {str(e)}",
                recommendation="Verify table exists and you have DESCRIBE permissions"
            )
            return False, "error"
            
    def check_table_read_access(self, database: str, table: str) -> bool:
        """
        Test reading from a Lake Formation protected table.
        
        This validates that:
        1. Lake Formation permissions are granted
        2. FGAC filtering is working
        3. No access errors occur
        """
        try:
            # Attempt to read from the table
            df = self.spark.sql(f"SELECT * FROM {database}.{table} LIMIT 10")
            row_count = df.count()
            columns = df.columns
            
            self.add_result(
                "Table Read Access Test",
                DiagnosticStatus.PASS,
                f"Successfully read from {database}.{table}",
                {
                    "rows_returned": row_count,
                    "columns_visible": columns,
                    "column_count": len(columns)
                }
            )
            return True
            
        except AnalysisException as e:
            error_msg = str(e)
            
            # Check for common Lake Formation errors
            if "AccessDeniedException" in error_msg or "not authorized" in error_msg.lower():
                self.add_result(
                    "Table Read Access Test",
                    DiagnosticStatus.FAIL,
                    f"Lake Formation denied access to {database}.{table}",
                    {"error": error_msg},
                    "Grant SELECT permission in Lake Formation for the job's IAM role"
                )
            elif "Table or view not found" in error_msg:
                self.add_result(
                    "Table Read Access Test",
                    DiagnosticStatus.FAIL,
                    f"Table {database}.{table} not found",
                    {"error": error_msg},
                    "Verify the table exists in the Glue Data Catalog"
                )
            else:
                self.add_result(
                    "Table Read Access Test",
                    DiagnosticStatus.FAIL,
                    f"Error reading table: {error_msg}",
                    recommendation="Check Lake Formation permissions and table configuration"
                )
            return False
            
        except Exception as e:
            self.add_result(
                "Table Read Access Test",
                DiagnosticStatus.FAIL,
                f"Unexpected error reading table: {str(e)}"
            )
            return False
            
    def check_column_level_filtering(self, database: str, table: str) -> bool:
        """
        Test that column-level FGAC is working.
        
        Compares available columns against expected columns to detect filtering.
        """
        try:
            # Get schema from Spark
            df = self.spark.sql(f"SELECT * FROM {database}.{table} LIMIT 0")
            visible_columns = set(df.columns)
            
            # Get full schema from Glue Catalog
            full_schema = self.spark.sql(f"DESCRIBE {database}.{table}")
            catalog_columns = set(
                row["col_name"] for row in full_schema.collect() 
                if row["col_name"] and not row["col_name"].startswith("#")
            )
            
            hidden_columns = catalog_columns - visible_columns
            
            result_details = {
                "visible_columns": list(visible_columns),
                "catalog_columns": list(catalog_columns),
                "potentially_filtered": list(hidden_columns)
            }
            
            if hidden_columns:
                self.add_result(
                    "Column-Level FGAC Test",
                    DiagnosticStatus.INFO,
                    f"Column filtering detected: {len(hidden_columns)} columns hidden",
                    result_details
                )
            else:
                self.add_result(
                    "Column-Level FGAC Test",
                    DiagnosticStatus.INFO,
                    "No column filtering detected (all columns visible)",
                    result_details
                )
                
            return True
            
        except Exception as e:
            self.add_result(
                "Column-Level FGAC Test",
                DiagnosticStatus.WARN,
                f"Could not verify column filtering: {str(e)}"
            )
            return True
            
    def check_row_level_filtering(self, database: str, table: str) -> bool:
        """
        Test row-level security by comparing counts.
        
        Note: This is informational only - we can't definitively know
        if rows are being filtered without admin access.
        """
        try:
            df = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {database}.{table}")
            row_count = df.collect()[0]["cnt"]
            
            self.add_result(
                "Row-Level FGAC Test",
                DiagnosticStatus.INFO,
                f"Table has {row_count} accessible rows",
                {"accessible_row_count": row_count},
                "Compare with expected count to verify row filtering is working"
            )
            return True
            
        except Exception as e:
            self.add_result(
                "Row-Level FGAC Test",
                DiagnosticStatus.WARN,
                f"Could not check row count: {str(e)}"
            )
            return True
            
    def check_unsupported_features(self) -> None:
        """
        Check for usage of features not supported with FGAC.
        
        Unsupported features:
        - RDD operations
        - Spark Streaming
        - Write with Lake Formation permissions
        - Nested column access control
        - UDTs, HiveUDFs, custom classes
        - Custom data sources
        - Additional jars for Spark extensions
        - ANALYZE TABLE command
        """
        unsupported_features = [
            {
                "feature": "RDD Operations",
                "description": "Resilient Distributed Datasets are not supported with FGAC",
                "detection": "Manual code review required"
            },
            {
                "feature": "Spark Streaming",
                "description": "Streaming is not supported with FGAC",
                "detection": "Manual code review required"
            },
            {
                "feature": "Write Operations via Lake Formation",
                "description": "Write operations use IAM permissions, not Lake Formation grants",
                "detection": "Ensure IAM role has S3 write permissions for write operations"
            },
            {
                "feature": "Nested Column Access Control",
                "description": "FGAC does not support access control on nested/struct columns",
                "detection": "Check table schema for nested types"
            },
            {
                "feature": "Custom UDTs/HiveUDFs",
                "description": "User-defined types and Hive UDFs are blocked",
                "detection": "Manual code review required"
            },
            {
                "feature": "Custom Data Sources",
                "description": "Custom data sources are not supported",
                "detection": "Manual code review required"
            },
            {
                "feature": "ANALYZE TABLE",
                "description": "ANALYZE TABLE command is blocked with FGAC",
                "detection": "Avoid using ANALYZE TABLE in FGAC-enabled jobs"
            }
        ]
        
        self.add_result(
            "Unsupported Features Check",
            DiagnosticStatus.INFO,
            "Review job code for unsupported FGAC features",
            {"unsupported_features": unsupported_features},
            "Avoid: RDD, Streaming, UDTs, HiveUDFs, custom data sources, ANALYZE TABLE"
        )
        
    def check_cross_account_access(self, database: str, table: str) -> bool:
        """
        Check if table is accessed via cross-account resource link.
        
        Limitation: Resource links must be named identically to source account's resource.
        """
        try:
            # Get table properties to check for resource link
            table_props = self.spark.sql(f"DESCRIBE EXTENDED {database}.{table}")
            props_dict = {row["col_name"]: row["data_type"] 
                         for row in table_props.collect()}
            
            # Check for cross-account indicators
            location = props_dict.get("Location", "")
            
            if "arn:aws" in location and location.count(":") >= 5:
                # Potentially cross-account
                self.add_result(
                    "Cross-Account Access Check",
                    DiagnosticStatus.INFO,
                    "Table may be accessed cross-account",
                    {"location": location},
                    "Ensure resource link name matches source table name exactly"
                )
            else:
                self.add_result(
                    "Cross-Account Access Check",
                    DiagnosticStatus.INFO,
                    "Table appears to be in same account",
                    {"location": location}
                )
                
            return True
            
        except Exception as e:
            self.add_result(
                "Cross-Account Access Check",
                DiagnosticStatus.WARN,
                f"Could not check cross-account status: {str(e)}"
            )
            return True
            
    def check_iceberg_specific(self, database: str, table: str) -> None:
        """
        Check Iceberg-specific limitations for FGAC.
        
        - Only session catalog is supported
        - Limited metadata tables
        - Hidden sensitive columns in metadata
        - register_table and migrate procedures not supported
        """
        # Check catalog configuration
        try:
            catalog_impl = self.spark.conf.get(
                "spark.sql.catalog.spark_catalog.catalog-impl", 
                "not_configured"
            )
            
            if "GlueCatalog" in catalog_impl or "iceberg" in catalog_impl.lower():
                self.add_result(
                    "Iceberg Catalog Check",
                    DiagnosticStatus.PASS,
                    "Session catalog is configured for Iceberg",
                    {"catalog_impl": catalog_impl}
                )
            else:
                self.add_result(
                    "Iceberg Catalog Check",
                    DiagnosticStatus.WARN,
                    "Iceberg catalog may not be properly configured",
                    {"catalog_impl": catalog_impl},
                    "Use spark_catalog (session catalog) for Iceberg with FGAC"
                )
                
        except Exception as e:
            self.add_result(
                "Iceberg Catalog Check",
                DiagnosticStatus.WARN,
                f"Could not verify Iceberg catalog config: {str(e)}"
            )
            
        # Document metadata table limitations
        self.add_result(
            "Iceberg Metadata Tables",
            DiagnosticStatus.INFO,
            "Only specific metadata tables are accessible with FGAC",
            {
                "supported_metadata_tables": self.SUPPORTED_ICEBERG_METADATA_TABLES,
                "hidden_columns": self.HIDDEN_ICEBERG_COLUMNS
            }
        )
        
        # Document unsupported procedures
        self.add_result(
            "Iceberg Procedures",
            DiagnosticStatus.INFO,
            "Some Iceberg procedures are not supported with FGAC",
            {"unsupported_procedures": ["register_table", "migrate"]}
        )
        
    def check_spark_session_limitation(self) -> None:
        """
        Document single Spark session limitation.
        
        AWS Glue with Lake Formation only supports a single Spark session.
        """
        self.add_result(
            "Spark Session Limitation",
            DiagnosticStatus.INFO,
            "FGAC only supports a single Spark session per job",
            recommendation="Do not create multiple SparkSession instances in your job"
        )
        
    def run_all_diagnostics(self, database: Optional[str] = None, 
                           table: Optional[str] = None) -> DiagnosticReport:
        """
        Run all diagnostic checks and return a comprehensive report.
        """
        logger.info("=" * 60)
        logger.info("Starting Lake Formation FGAC Diagnostics")
        logger.info("=" * 60)
        
        # Core infrastructure checks
        self.check_glue_version()
        self.check_fgac_enabled()
        self.check_worker_allocation()
        self.check_region_support()
        self.check_iam_permissions()
        self.check_spark_session_limitation()
        self.check_unsupported_features()
        
        # Table-specific checks if database/table provided
        if database and table:
            logger.info(f"Running table-specific diagnostics for {database}.{table}")
            
            supported, table_format = self.check_table_format(database, table)
            
            if supported:
                self.check_table_read_access(database, table)
                self.check_column_level_filtering(database, table)
                self.check_row_level_filtering(database, table)
                self.check_cross_account_access(database, table)
                
                if table_format == "iceberg":
                    self.check_iceberg_specific(database, table)
        else:
            # List available databases and tables
            self.list_available_tables()
            
        # Generate report
        passed = sum(1 for r in self.results if r.status == DiagnosticStatus.PASS.value)
        failed = sum(1 for r in self.results if r.status == DiagnosticStatus.FAIL.value)
        warnings = sum(1 for r in self.results if r.status == DiagnosticStatus.WARN.value)
        
        report = DiagnosticReport(
            job_name=self.job_name,
            run_id=self.run_id,
            timestamp=datetime.utcnow().isoformat(),
            glue_version=self.spark.conf.get("spark.glue.version", "5.0"),
            total_checks=len(self.results),
            passed=passed,
            failed=failed,
            warnings=warnings,
            results=[asdict(r) for r in self.results]
        )
        
        # Log summary
        logger.info("=" * 60)
        logger.info("DIAGNOSTIC SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Checks: {report.total_checks}")
        logger.info(f"Passed: {passed}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Warnings: {warnings}")
        logger.info("=" * 60)
        
        return report
        
    def list_available_tables(self) -> None:
        """List available databases and tables for reference."""
        try:
            # Use SQL instead of spark.catalog.* (which uses RDDs blocked by FGAC)
            databases_df = self.spark.sql("SHOW DATABASES")
            db_names = [row["databaseName"] for row in databases_df.collect()]
            db_tables = {}

            for db_name in db_names:
                try:
                    tables_df = self.spark.sql(f"SHOW TABLES IN `{db_name}`")
                    db_tables[db_name] = [row["tableName"] for row in tables_df.collect()]
                except Exception:
                    db_tables[db_name] = ["<access denied>"]

            self.add_result(
                "Available Tables",
                DiagnosticStatus.INFO,
                f"Found {len(db_tables)} databases in catalog",
                {"databases": db_tables}
            )

        except Exception as e:
            self.add_result(
                "Available Tables",
                DiagnosticStatus.WARN,
                f"Could not list tables: {str(e)}"
            )


def save_report_to_s3(spark: SparkSession, report: DiagnosticReport, 
                      output_path: str) -> None:
    """Save the diagnostic report to S3 as JSON."""
    import json
    
    report_json = json.dumps(asdict(report), indent=2, default=str)
    
    # Create DataFrame and write to S3
    report_df = spark.createDataFrame([(report_json,)], ["report"])
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    full_path = f"{output_path}/lf_fgac_diagnostic_{timestamp}"
    
    report_df.write.mode("overwrite").text(full_path)
    logger.info(f"Report saved to: {full_path}")


def main():
    """Main entry point for the diagnostic job."""
    
    # Parse job arguments
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
    ])
    
    # Get optional arguments with defaults
    optional_args = {}
    for arg in ["diagnostic_database", "diagnostic_table", "output_location"]:
        try:
            optional_args.update(getResolvedOptions(sys.argv, [arg]))
        except Exception:
            pass
    
    args.update(optional_args)
    
    # Initialize Spark and Glue context
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    
    try:
        # Create diagnostics instance
        diagnostics = LakeFormationFGACDiagnostics(spark, glue_context, args)
        
        # Run diagnostics
        database = args.get("diagnostic_database")
        table = args.get("diagnostic_table")
        
        report = diagnostics.run_all_diagnostics(database, table)
        
        # Save report if output location specified
        output_location = args.get("output_location")
        if output_location:
            save_report_to_s3(spark, report, output_location)
            
        # Print full report to logs
        logger.info("\nFULL DIAGNOSTIC REPORT:")
        logger.info(json.dumps(asdict(report), indent=2, default=str))
        
        # Fail job if critical issues found
        if report.failed > 0:
            logger.error(f"Diagnostic found {report.failed} FAILED checks")
            # Don't raise error - let job complete to show full report
            
    except Exception as e:
        logger.error(f"Diagnostic job failed: {str(e)}")
        raise
        
    finally:
        job.commit()


if __name__ == "__main__":
    main()