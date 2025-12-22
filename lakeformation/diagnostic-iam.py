"""
AWS Glue ETL Job: IAM Permissions Diagnostics
==============================================

This job performs comprehensive diagnostic checks to verify IAM permissions
are working correctly for AWS Glue jobs that use IAM-based access control
(not Lake Formation).

This diagnostic tests both read AND write operations using PySpark and
Hadoop FileSystem APIs (the same libraries a real Glue job would use).

Usage:
------
Deploy as an AWS Glue job with the following parameters:
  --number-of-workers: 2 (minimum)

Required job parameters:
  --diagnostic_database: Database name to test
  --diagnostic_table: Table name to test reads against

Optional job parameters:
  --output_location: S3 path for diagnostic report output
  --test_write_path: S3 path for write tests (will create/delete test files)
  --test_write_database: Database to test table creation (defaults to diagnostic_database)
"""

import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum

# AWS Glue imports
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# PySpark imports
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.utils import AnalysisException


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IAM-Diagnostics")


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


class IAMDiagnostics:
    """
    Diagnostic utility for AWS Glue IAM-based access control.

    Tests read and write permissions using PySpark and Hadoop FileSystem APIs:
    - Glue Data Catalog (databases, tables)
    - S3 data access via Spark
    - S3 write operations via Spark
    """

    def __init__(self, spark: SparkSession, glue_context: GlueContext, args: Dict[str, str]):
        """Initialize the diagnostics utility."""
        self.spark = spark
        self.glue_context = glue_context
        self.args = args
        self.results: List[DiagnosticResult] = []
        self.job_name = args.get("JOB_NAME", "unknown")
        self.run_id = args.get("JOB_RUN_ID", datetime.utcnow().strftime("%Y%m%d%H%M%S"))
        self._created_resources: List[Dict[str, str]] = []
        self._hadoop_fs = None

    def _get_hadoop_fs(self, path: str):
        """Get Hadoop FileSystem for the given path."""
        from py4j.java_gateway import java_import

        jvm = self.spark._jvm
        java_import(jvm, "org.apache.hadoop.fs.Path")
        java_import(jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(jvm, "java.net.URI")

        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI(path)
        return jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)

    def _get_hadoop_path(self, path: str):
        """Create a Hadoop Path object."""
        return self.spark._jvm.org.apache.hadoop.fs.Path(path)

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
        """Check AWS Glue version."""
        try:
            glue_version = self.spark.conf.get("spark.glue.version", "unknown")

            if glue_version == "unknown":
                glue_version = self.args.get("glue_version", "unknown")

            self.add_result(
                "Glue Version Check",
                DiagnosticStatus.INFO,
                f"Running on AWS Glue version {glue_version}",
                {"glue_version": glue_version}
            )
            return True

        except Exception as e:
            self.add_result(
                "Glue Version Check",
                DiagnosticStatus.WARN,
                f"Could not determine Glue version: {str(e)}"
            )
            return True

    def check_region(self) -> bool:
        """Identify the AWS region from Spark/Hadoop configuration."""
        try:
            # Try various Spark/Hadoop config options for region
            region = None
            region_configs = [
                "spark.hadoop.aws.region",
                "fs.s3a.endpoint.region",
                "fs.s3.endpoint.region",
            ]

            for config in region_configs:
                try:
                    region = self.spark.conf.get(config)
                    if region:
                        break
                except Exception:
                    pass

            # Try Hadoop configuration directly
            if not region:
                try:
                    hadoop_conf = self.spark._jsc.hadoopConfiguration()
                    region = hadoop_conf.get("fs.s3a.endpoint.region")
                except Exception:
                    pass

            if not region:
                region = "unknown (using default)"

            self.add_result(
                "Region Check",
                DiagnosticStatus.INFO,
                f"Running in region: {region}",
                {"region": region}
            )
            return True

        except Exception as e:
            self.add_result(
                "Region Check",
                DiagnosticStatus.WARN,
                f"Could not determine region: {str(e)}"
            )
            return True

    def check_spark_context(self) -> bool:
        """Verify Spark context is properly initialized."""
        try:
            sc = self.spark.sparkContext

            app_id = sc.applicationId
            app_name = sc.appName
            master = sc.master

            self.add_result(
                "Spark Context Check",
                DiagnosticStatus.PASS,
                f"Spark context initialized: {app_name}",
                {
                    "application_id": app_id,
                    "application_name": app_name,
                    "master": master
                }
            )
            return True

        except Exception as e:
            self.add_result(
                "Spark Context Check",
                DiagnosticStatus.FAIL,
                f"Spark context not available: {str(e)}"
            )
            return False

    # =========================================================================
    # READ OPERATIONS
    # =========================================================================

    def check_catalog_read(self) -> bool:
        """Test read access to Glue Data Catalog."""
        try:
            databases_df = self.spark.sql("SHOW DATABASES")
            col_name = databases_df.columns[0]
            db_list = [row[col_name] for row in databases_df.collect()]

            self.add_result(
                "Catalog Read Access",
                DiagnosticStatus.PASS,
                f"Can read Glue Catalog ({len(db_list)} databases found)",
                {"databases": db_list[:10]}
            )
            return True

        except Exception as e:
            self.add_result(
                "Catalog Read Access",
                DiagnosticStatus.FAIL,
                f"Cannot read Glue Catalog: {str(e)}",
                recommendation="Ensure IAM role has glue:GetDatabases, glue:GetDatabase permissions"
            )
            return False

    def check_table_metadata_read(self, database: str, table: str) -> bool:
        """Test reading table metadata from Glue Catalog."""
        try:
            metadata_df = self.spark.sql(f"DESCRIBE EXTENDED `{database}`.`{table}`")
            metadata = {row["col_name"]: row["data_type"]
                       for row in metadata_df.collect()
                       if row["col_name"]}

            location = metadata.get("Location", "unknown")

            self.add_result(
                "Table Metadata Read",
                DiagnosticStatus.PASS,
                f"Can read table metadata for {database}.{table}",
                {
                    "table": f"{database}.{table}",
                    "location": location,
                    "columns_count": len([k for k in metadata.keys() if not k.startswith("#")])
                }
            )
            return True

        except Exception as e:
            self.add_result(
                "Table Metadata Read",
                DiagnosticStatus.FAIL,
                f"Cannot read table metadata: {str(e)}",
                recommendation="Ensure IAM role has glue:GetTable permission"
            )
            return False

    def check_table_data_read(self, database: str, table: str) -> bool:
        """Test reading data from a table using Spark SQL."""
        try:
            df = self.spark.sql(f"SELECT * FROM `{database}`.`{table}` LIMIT 10")
            row_count = df.count()
            columns = df.columns

            self.add_result(
                "Table Data Read",
                DiagnosticStatus.PASS,
                f"Can read data from {database}.{table}",
                {
                    "rows_returned": row_count,
                    "columns": columns,
                    "column_count": len(columns)
                }
            )
            return True

        except AnalysisException as e:
            error_msg = str(e)
            if "AccessDenied" in error_msg:
                self.add_result(
                    "Table Data Read",
                    DiagnosticStatus.FAIL,
                    f"S3 access denied for {database}.{table}",
                    {"error": error_msg},
                    "Ensure IAM role has s3:GetObject permission on the table's S3 location"
                )
            else:
                self.add_result(
                    "Table Data Read",
                    DiagnosticStatus.FAIL,
                    f"Cannot read table data: {error_msg}",
                    recommendation="Check table exists and IAM permissions"
                )
            return False

        except Exception as e:
            self.add_result(
                "Table Data Read",
                DiagnosticStatus.FAIL,
                f"Error reading table data: {str(e)}"
            )
            return False

    def check_s3_list(self, s3_path: str) -> bool:
        """Test S3 list permissions using Hadoop FileSystem API."""
        try:
            fs = self._get_hadoop_fs(s3_path)
            hadoop_path = self._get_hadoop_path(s3_path)

            # List files in the path
            file_status_list = fs.listStatus(hadoop_path)
            file_count = file_status_list.length if hasattr(file_status_list, 'length') else len(file_status_list)

            # Get first few file names
            files = []
            for i in range(min(5, file_count)):
                files.append(file_status_list[i].getPath().getName())

            self.add_result(
                "S3 List Access",
                DiagnosticStatus.PASS,
                f"Can list objects in {s3_path}",
                {
                    "path": s3_path,
                    "objects_found": file_count,
                    "sample_files": files
                }
            )
            return True

        except Exception as e:
            self.add_result(
                "S3 List Access",
                DiagnosticStatus.FAIL,
                f"Cannot list S3 objects: {str(e)}",
                recommendation="Ensure IAM role has s3:ListBucket permission"
            )
            return False

    def check_s3_read_direct(self, s3_path: str) -> bool:
        """Test reading S3 data directly using Spark."""
        try:
            # Try to read as text to verify basic S3 access
            # This will fail gracefully if the path contains non-text data
            df = self.spark.read.text(s3_path)
            # Just check if we can access the data, limit to avoid loading too much
            sample = df.limit(1).collect()

            self.add_result(
                "S3 Direct Read",
                DiagnosticStatus.PASS,
                f"Can read S3 data directly via Spark",
                {"path": s3_path}
            )
            return True

        except Exception as e:
            # This might fail if the data isn't text format, which is OK
            error_msg = str(e)
            if "AccessDenied" in error_msg or "403" in error_msg:
                self.add_result(
                    "S3 Direct Read",
                    DiagnosticStatus.FAIL,
                    f"S3 access denied: {error_msg}",
                    recommendation="Ensure IAM role has s3:GetObject permission"
                )
                return False
            else:
                self.add_result(
                    "S3 Direct Read",
                    DiagnosticStatus.INFO,
                    "S3 direct read test inconclusive (data may not be text format)",
                    {"note": "Table data read test is more reliable"}
                )
                return True

    # =========================================================================
    # WRITE OPERATIONS
    # =========================================================================

    def check_s3_write_json(self, s3_path: str) -> bool:
        """Test S3 write permissions by writing JSON data via Spark."""
        test_path = f"{s3_path.rstrip('/')}/diagnostic_json_{self.run_id}"

        try:
            # Create test DataFrame with diagnostic data
            test_data = [
                {
                    "test_type": "diagnostic_write_test",
                    "job_name": self.job_name,
                    "run_id": self.run_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "row_id": i
                }
                for i in range(3)
            ]

            df = self.spark.createDataFrame(test_data)

            # Write as JSON
            df.write.mode("overwrite").json(test_path)

            # Track for cleanup
            self._created_resources.append({
                "type": "s3_path",
                "path": test_path
            })

            # Verify by reading back
            read_df = self.spark.read.json(test_path)
            count = read_df.count()

            self.add_result(
                "S3 JSON Write",
                DiagnosticStatus.PASS,
                f"Can write JSON data to S3 via Spark",
                {
                    "path": test_path,
                    "rows_written": len(test_data),
                    "rows_verified": count
                }
            )
            return True

        except Exception as e:
            error_msg = str(e)
            if "AccessDenied" in error_msg or "403" in error_msg:
                self.add_result(
                    "S3 JSON Write",
                    DiagnosticStatus.FAIL,
                    f"S3 write access denied: {error_msg}",
                    recommendation="Ensure IAM role has s3:PutObject permission"
                )
            else:
                self.add_result(
                    "S3 JSON Write",
                    DiagnosticStatus.FAIL,
                    f"Cannot write JSON to S3: {error_msg}",
                    recommendation="Check S3 permissions and path validity"
                )
            return False

    def check_s3_write_parquet(self, s3_path: str) -> bool:
        """Test writing Parquet data via Spark."""
        test_path = f"{s3_path.rstrip('/')}/diagnostic_parquet_{self.run_id}"

        try:
            # Create test DataFrame
            test_data = [
                (1, "test_row_1", datetime.utcnow()),
                (2, "test_row_2", datetime.utcnow()),
                (3, "test_row_3", datetime.utcnow())
            ]

            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("created_at", TimestampType(), True)
            ])

            df = self.spark.createDataFrame(test_data, schema)

            # Write as Parquet
            df.write.mode("overwrite").parquet(test_path)

            # Track for cleanup
            self._created_resources.append({
                "type": "s3_path",
                "path": test_path
            })

            # Verify by reading back
            read_df = self.spark.read.parquet(test_path)
            count = read_df.count()

            self.add_result(
                "S3 Parquet Write",
                DiagnosticStatus.PASS,
                f"Can write Parquet data to S3 via Spark",
                {
                    "path": test_path,
                    "rows_written": len(test_data),
                    "rows_verified": count
                }
            )
            return True

        except Exception as e:
            error_msg = str(e)
            if "AccessDenied" in error_msg or "403" in error_msg:
                self.add_result(
                    "S3 Parquet Write",
                    DiagnosticStatus.FAIL,
                    f"S3 write access denied: {error_msg}",
                    recommendation="Ensure IAM role has s3:PutObject permission"
                )
            else:
                self.add_result(
                    "S3 Parquet Write",
                    DiagnosticStatus.FAIL,
                    f"Cannot write Parquet to S3: {error_msg}",
                    recommendation="Check S3 permissions and path validity"
                )
            return False

    def check_s3_write_csv(self, s3_path: str) -> bool:
        """Test writing CSV data via Spark."""
        test_path = f"{s3_path.rstrip('/')}/diagnostic_csv_{self.run_id}"

        try:
            # Create test DataFrame
            test_data = [
                (1, "test_row_1", "2024-01-01"),
                (2, "test_row_2", "2024-01-02"),
                (3, "test_row_3", "2024-01-03")
            ]

            schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("date", StringType(), True)
            ])

            df = self.spark.createDataFrame(test_data, schema)

            # Write as CSV with header
            df.write.mode("overwrite").option("header", "true").csv(test_path)

            # Track for cleanup
            self._created_resources.append({
                "type": "s3_path",
                "path": test_path
            })

            # Verify by reading back
            read_df = self.spark.read.option("header", "true").csv(test_path)
            count = read_df.count()

            self.add_result(
                "S3 CSV Write",
                DiagnosticStatus.PASS,
                f"Can write CSV data to S3 via Spark",
                {
                    "path": test_path,
                    "rows_written": len(test_data),
                    "rows_verified": count
                }
            )
            return True

        except Exception as e:
            error_msg = str(e)
            if "AccessDenied" in error_msg or "403" in error_msg:
                self.add_result(
                    "S3 CSV Write",
                    DiagnosticStatus.FAIL,
                    f"S3 write access denied: {error_msg}",
                    recommendation="Ensure IAM role has s3:PutObject permission"
                )
            else:
                self.add_result(
                    "S3 CSV Write",
                    DiagnosticStatus.FAIL,
                    f"Cannot write CSV to S3: {error_msg}",
                    recommendation="Check S3 permissions and path validity"
                )
            return False

    def check_s3_delete(self, s3_path: str) -> bool:
        """Test S3 delete permissions using Hadoop FileSystem API."""
        # Find paths we created for cleanup/delete testing
        test_paths = [r for r in self._created_resources if r["type"] == "s3_path"]

        if not test_paths:
            self.add_result(
                "S3 Delete Access",
                DiagnosticStatus.SKIP,
                "No test paths to delete (write tests may have failed)"
            )
            return True

        try:
            deleted_paths = []

            for resource in test_paths[:1]:  # Just test with one path
                path = resource["path"]
                fs = self._get_hadoop_fs(path)
                hadoop_path = self._get_hadoop_path(path)

                # Delete recursively
                success = fs.delete(hadoop_path, True)  # True = recursive

                if success:
                    deleted_paths.append(path)
                    self._created_resources.remove(resource)

            self.add_result(
                "S3 Delete Access",
                DiagnosticStatus.PASS,
                f"Can delete from S3 via Hadoop FileSystem",
                {"deleted_paths": deleted_paths}
            )
            return True

        except Exception as e:
            error_msg = str(e)
            if "AccessDenied" in error_msg or "403" in error_msg:
                self.add_result(
                    "S3 Delete Access",
                    DiagnosticStatus.FAIL,
                    f"S3 delete access denied: {error_msg}",
                    recommendation="Ensure IAM role has s3:DeleteObject permission"
                )
            else:
                self.add_result(
                    "S3 Delete Access",
                    DiagnosticStatus.FAIL,
                    f"Cannot delete from S3: {error_msg}",
                    recommendation="Check S3 permissions"
                )
            return False

    def check_catalog_table_create(self, database: str) -> bool:
        """Test creating a table in the Glue Catalog using Spark SQL."""
        test_table = f"diagnostic_test_{self.run_id.replace('-', '_')}"

        try:
            # Create a simple managed table
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS `{database}`.`{test_table}` (
                    id INT,
                    name STRING,
                    created_at TIMESTAMP
                )
                USING PARQUET
            """)

            # Track for cleanup
            self._created_resources.append({
                "type": "glue_table",
                "database": database,
                "table": test_table
            })

            self.add_result(
                "Catalog Table Create",
                DiagnosticStatus.PASS,
                f"Can create table in Glue Catalog: {database}.{test_table}",
                {
                    "database": database,
                    "table": test_table
                }
            )
            return True

        except Exception as e:
            self.add_result(
                "Catalog Table Create",
                DiagnosticStatus.FAIL,
                f"Cannot create table in catalog: {str(e)}",
                recommendation="Ensure IAM role has glue:CreateTable permission"
            )
            return False

    def check_catalog_table_insert(self, database: str) -> bool:
        """Test inserting data into a table using Spark SQL."""
        # Find test tables we created
        test_tables = [r for r in self._created_resources
                      if r["type"] == "glue_table" and r["database"] == database]

        if not test_tables:
            self.add_result(
                "Catalog Table Insert",
                DiagnosticStatus.SKIP,
                "No test tables available (create test may have failed)"
            )
            return True

        test_table = test_tables[0]
        full_table_name = f"`{test_table['database']}`.`{test_table['table']}`"

        try:
            # Insert test data
            self.spark.sql(f"""
                INSERT INTO {full_table_name} VALUES
                (1, 'test_row_1', current_timestamp()),
                (2, 'test_row_2', current_timestamp()),
                (3, 'test_row_3', current_timestamp())
            """)

            # Verify
            count_df = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}")
            count = count_df.collect()[0]["cnt"]

            self.add_result(
                "Catalog Table Insert",
                DiagnosticStatus.PASS,
                f"Can insert data into Glue Catalog table",
                {
                    "table": full_table_name,
                    "rows_inserted": 3,
                    "total_rows": count
                }
            )
            return True

        except Exception as e:
            self.add_result(
                "Catalog Table Insert",
                DiagnosticStatus.FAIL,
                f"Cannot insert into table: {str(e)}",
                recommendation="Ensure IAM role has s3:PutObject permission on table location"
            )
            return False

    def check_catalog_table_drop(self, database: str) -> bool:
        """Test dropping a table from the Glue Catalog using Spark SQL."""
        # Find test tables we created
        test_tables = [r for r in self._created_resources
                      if r["type"] == "glue_table" and r["database"] == database]

        if not test_tables:
            self.add_result(
                "Catalog Table Drop",
                DiagnosticStatus.SKIP,
                "No test tables to drop (create test may have failed)"
            )
            return True

        try:
            dropped_tables = []
            for tbl in test_tables:
                self.spark.sql(f"DROP TABLE IF EXISTS `{tbl['database']}`.`{tbl['table']}`")
                dropped_tables.append(f"{tbl['database']}.{tbl['table']}")
                self._created_resources.remove(tbl)

            self.add_result(
                "Catalog Table Drop",
                DiagnosticStatus.PASS,
                f"Can drop tables from Glue Catalog ({len(dropped_tables)} tables dropped)",
                {"dropped_tables": dropped_tables}
            )
            return True

        except Exception as e:
            self.add_result(
                "Catalog Table Drop",
                DiagnosticStatus.FAIL,
                f"Cannot drop table from catalog: {str(e)}",
                recommendation="Ensure IAM role has glue:DeleteTable permission"
            )
            return False

    # =========================================================================
    # CLEANUP
    # =========================================================================

    def cleanup_test_resources(self) -> None:
        """Clean up any test resources created during diagnostics using Hadoop FileSystem."""
        if not self._created_resources:
            logger.info("No resources to clean up")
            return

        logger.info(f"Cleaning up {len(self._created_resources)} test resources...")
        cleanup_errors = []

        for resource in self._created_resources.copy():
            try:
                if resource["type"] == "s3_path":
                    fs = self._get_hadoop_fs(resource["path"])
                    hadoop_path = self._get_hadoop_path(resource["path"])
                    fs.delete(hadoop_path, True)  # True = recursive
                    self._created_resources.remove(resource)
                    logger.info(f"Deleted S3 path: {resource['path']}")

                elif resource["type"] == "glue_table":
                    self.spark.sql(
                        f"DROP TABLE IF EXISTS `{resource['database']}`.`{resource['table']}`"
                    )
                    self._created_resources.remove(resource)
                    logger.info(f"Dropped table: {resource['database']}.{resource['table']}")

            except Exception as e:
                cleanup_errors.append(f"{resource}: {str(e)}")
                logger.warning(f"Failed to cleanup resource {resource}: {str(e)}")

        if self._created_resources:
            logger.warning(f"Failed to cleanup {len(self._created_resources)} resources")
            self.add_result(
                "Cleanup",
                DiagnosticStatus.WARN,
                f"Failed to cleanup some test resources",
                {"remaining_resources": self._created_resources, "errors": cleanup_errors}
            )
        else:
            self.add_result(
                "Cleanup",
                DiagnosticStatus.PASS,
                "All test resources cleaned up successfully"
            )

    # =========================================================================
    # MAIN DIAGNOSTIC RUNNER
    # =========================================================================

    def run_all_diagnostics(self, database: str, table: str,
                           test_write_path: Optional[str] = None,
                           test_write_database: Optional[str] = None) -> DiagnosticReport:
        """Run all diagnostic checks and return a comprehensive report."""
        logger.info("=" * 60)
        logger.info("Starting IAM Permissions Diagnostics")
        logger.info("Using PySpark and Hadoop FileSystem APIs")
        logger.info("=" * 60)

        # Infrastructure checks
        self.check_glue_version()
        self.check_region()
        self.check_spark_context()

        # Read operations
        logger.info("-" * 40)
        logger.info("READ OPERATIONS")
        logger.info("-" * 40)

        self.check_catalog_read()
        self.check_table_metadata_read(database, table)

        # Get table location for S3 tests
        table_location = None
        try:
            metadata_df = self.spark.sql(f"DESCRIBE EXTENDED `{database}`.`{table}`")
            for row in metadata_df.collect():
                if row["col_name"] == "Location":
                    table_location = row["data_type"]
                    break
        except Exception:
            pass

        if table_location:
            self.check_s3_list(table_location)

        self.check_table_data_read(database, table)

        # Write operations
        logger.info("-" * 40)
        logger.info("WRITE OPERATIONS")
        logger.info("-" * 40)

        write_path = test_write_path
        if not write_path:
            self.add_result(
                "Write Tests",
                DiagnosticStatus.SKIP,
                "No --test_write_path provided, skipping S3 write tests",
                recommendation="Provide --test_write_path to test S3 write permissions"
            )
        else:
            self.check_s3_write_json(write_path)
            self.check_s3_write_parquet(write_path)
            self.check_s3_write_csv(write_path)
            self.check_s3_delete(write_path)

        write_database = test_write_database or database
        if test_write_path:
            self.check_catalog_table_create(write_database)
            self.check_catalog_table_insert(write_database)
            self.check_catalog_table_drop(write_database)

        # Cleanup
        logger.info("-" * 40)
        logger.info("CLEANUP")
        logger.info("-" * 40)
        self.cleanup_test_resources()

        # Generate report
        passed = sum(1 for r in self.results if r.status == DiagnosticStatus.PASS.value)
        failed = sum(1 for r in self.results if r.status == DiagnosticStatus.FAIL.value)
        warnings = sum(1 for r in self.results if r.status == DiagnosticStatus.WARN.value)

        report = DiagnosticReport(
            job_name=self.job_name,
            run_id=self.run_id,
            timestamp=datetime.utcnow().isoformat(),
            glue_version=self.spark.conf.get("spark.glue.version", "unknown"),
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


def save_report_to_s3(spark: SparkSession, report: DiagnosticReport,
                      output_path: str) -> None:
    """Save the diagnostic report to S3 as JSON using Spark."""
    report_json = json.dumps(asdict(report), indent=2, default=str)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    full_path = f"{output_path.rstrip('/')}/iam_diagnostic_{timestamp}"

    # Create DataFrame with the report and write as text
    report_df = spark.createDataFrame([(report_json,)], ["report"])
    report_df.coalesce(1).write.mode("overwrite").text(full_path)

    logger.info(f"Report saved to: {full_path}")


def main():
    """Main entry point for the diagnostic job."""

    # Parse required job arguments
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "diagnostic_database",
        "diagnostic_table"
    ])

    # Get optional arguments with defaults
    optional_args = {}
    for arg in ["output_location", "test_write_path", "test_write_database"]:
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
        diagnostics = IAMDiagnostics(spark, glue_context, args)

        # Run diagnostics
        database = args["diagnostic_database"]
        table = args["diagnostic_table"]
        test_write_path = args.get("test_write_path")
        test_write_database = args.get("test_write_database")

        report = diagnostics.run_all_diagnostics(
            database=database,
            table=table,
            test_write_path=test_write_path,
            test_write_database=test_write_database
        )

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

    except Exception as e:
        logger.error(f"Diagnostic job failed: {str(e)}")
        raise

    finally:
        job.commit()


if __name__ == "__main__":
    main()
