"""
AWS Glue ETL Job: Lake Formation FGAC with Apache Iceberg
=========================================================

This example demonstrates best practices for building AWS Glue ETL jobs that use:
- Lake Formation Fine-Grained Access Control (FGAC)
- Apache Iceberg table format

IMPORTANT LIMITATIONS (Lake Formation FGAC):
--------------------------------------------
1. Requires AWS Glue 5.0+ with --enable-lakeformation-fine-grained-access=true
2. Minimum 4 workers required (1 user driver, 1 system driver, 1 system executor, 1 standby)
3. NO RDD operations - use DataFrame API only
4. NO spark.catalog.* methods - use SQL instead (SHOW DATABASES, SHOW TABLES, etc.)
5. NO Spark Streaming
6. Write operations use IAM permissions, NOT Lake Formation grants
7. NO nested/struct column access control
8. NO custom UDTs, HiveUDFs, or custom data sources
9. NO ANALYZE TABLE command
10. Single SparkSession only - do not create multiple sessions

ICEBERG-SPECIFIC LIMITATIONS (with FGAC):
-----------------------------------------
1. Only session catalog (spark_catalog) is supported
2. Limited metadata tables: history, metadata_log_entries, snapshots, files, manifests, refs
3. Hidden columns in metadata: partitions, path, summaries
4. Unsupported procedures: register_table, migrate

Usage:
------
Deploy as an AWS Glue job with:
  --enable-lakeformation-fine-grained-access: true
  --number-of-workers: 4 (minimum)
  --conf: spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
  --conf: spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
  --conf: spark.sql.catalog.spark_catalog.warehouse=s3://your-bucket/warehouse/
  --datalake-formats: iceberg

Required job parameters:
  --source_database: Source database name
  --source_table: Source table name
  --target_database: Target database name
  --target_table: Target Iceberg table name
  --warehouse_path: S3 path for Iceberg warehouse
"""

import sys
import logging
from datetime import datetime
from typing import Optional

# AWS Glue imports
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# PySpark imports - DataFrame API only (no RDD!)
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, year, month, dayofmonth,
    when, coalesce, hash as spark_hash
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LF-Iceberg-ETL")


class LakeFormationIcebergETL:
    """
    ETL processor that reads from Lake Formation protected tables
    and writes to Apache Iceberg tables.

    This class demonstrates FGAC-compliant patterns.
    """

    def __init__(self, spark: SparkSession, glue_context: GlueContext):
        """Initialize the ETL processor."""
        self.spark = spark
        self.glue_context = glue_context

    # =========================================================================
    # CATALOG OPERATIONS (FGAC-compliant - using SQL, not spark.catalog.*)
    # =========================================================================

    def list_databases(self) -> list:
        """
        List available databases using SQL.

        NOTE: Do NOT use spark.catalog.listDatabases() - it uses RDDs internally
        which are blocked by FGAC.
        """
        df = self.spark.sql("SHOW DATABASES")
        col_name = df.columns[0]  # Column name varies by Spark version
        return [row[col_name] for row in df.collect()]

    def list_tables(self, database: str) -> list:
        """
        List tables in a database using SQL.

        NOTE: Do NOT use spark.catalog.listTables() - it uses RDDs internally.
        """
        df = self.spark.sql(f"SHOW TABLES IN `{database}`")
        # SHOW TABLES returns: database, tableName, isTemporary
        table_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        return [row[table_col] for row in df.collect()]

    def table_exists(self, database: str, table: str) -> bool:
        """Check if a table exists using SQL."""
        try:
            tables = self.list_tables(database)
            return table in tables
        except Exception:
            return False

    def get_table_location(self, database: str, table: str) -> Optional[str]:
        """Get S3 location of a table using DESCRIBE."""
        try:
            df = self.spark.sql(f"DESCRIBE EXTENDED `{database}`.`{table}`")
            for row in df.collect():
                if row["col_name"] == "Location":
                    return row["data_type"]
            return None
        except Exception as e:
            logger.warning(f"Could not get table location: {e}")
            return None

    # =========================================================================
    # READ OPERATIONS (Lake Formation FGAC applies here)
    # =========================================================================

    def read_table(self, database: str, table: str) -> DataFrame:
        """
        Read from a Lake Formation protected table.

        Lake Formation FGAC automatically filters:
        - Rows (if row-level security is configured)
        - Columns (if column-level security is configured)

        The returned DataFrame only contains data the job's IAM role
        is authorized to see via Lake Formation grants.
        """
        logger.info(f"Reading from {database}.{table} (FGAC filtering applied)")

        # Use SQL for reading - fully FGAC compliant
        return self.spark.sql(f"SELECT * FROM `{database}`.`{table}`")

    def read_table_with_filter(self, database: str, table: str,
                                filter_condition: str) -> DataFrame:
        """
        Read with an additional filter condition.

        Note: Lake Formation row filters are applied BEFORE this filter.
        """
        logger.info(f"Reading from {database}.{table} with filter: {filter_condition}")
        return self.spark.sql(
            f"SELECT * FROM `{database}`.`{table}` WHERE {filter_condition}"
        )

    def read_incremental(self, database: str, table: str,
                         timestamp_column: str,
                         last_processed: datetime) -> DataFrame:
        """
        Read incremental changes since last processed timestamp.

        Common pattern for CDC/incremental ETL.
        """
        timestamp_str = last_processed.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Reading incremental data from {database}.{table} since {timestamp_str}")

        return self.spark.sql(f"""
            SELECT * FROM `{database}`.`{table}`
            WHERE `{timestamp_column}` > TIMESTAMP '{timestamp_str}'
        """)

    # =========================================================================
    # ICEBERG TABLE OPERATIONS
    # =========================================================================

    def create_iceberg_table(self, database: str, table: str,
                             schema: StructType, warehouse_path: str,
                             partition_columns: list = None,
                             comment: str = None) -> None:
        """
        Create an Apache Iceberg table in the Glue Catalog.

        NOTE: Table creation uses IAM permissions, not Lake Formation.
        Ensure the job's IAM role has glue:CreateTable permission.
        """
        location = f"{warehouse_path.rstrip('/')}/{database}/{table}"

        # Build column definitions
        columns = ", ".join([
            f"`{field.name}` {self._spark_type_to_sql(field.dataType)}"
            for field in schema.fields
        ])

        # Build PARTITIONED BY clause
        partition_clause = ""
        if partition_columns:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_columns)})"

        # Build comment
        comment_clause = f"COMMENT '{comment}'" if comment else ""

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (
                {columns}
            )
            USING ICEBERG
            {partition_clause}
            {comment_clause}
            LOCATION '{location}'
            TBLPROPERTIES (
                'format-version' = '2',
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'zstd'
            )
        """

        logger.info(f"Creating Iceberg table: {database}.{table}")
        self.spark.sql(create_sql)
        logger.info(f"Created Iceberg table at {location}")

    def _spark_type_to_sql(self, spark_type) -> str:
        """Convert Spark data type to SQL type string."""
        type_map = {
            "StringType": "STRING",
            "LongType": "BIGINT",
            "IntegerType": "INT",
            "DoubleType": "DOUBLE",
            "FloatType": "FLOAT",
            "BooleanType": "BOOLEAN",
            "TimestampType": "TIMESTAMP",
            "DateType": "DATE",
            "BinaryType": "BINARY",
            "DecimalType": "DECIMAL",
        }
        type_name = type(spark_type).__name__
        return type_map.get(type_name, "STRING")

    # =========================================================================
    # WRITE OPERATIONS (Uses IAM, NOT Lake Formation)
    # =========================================================================

    def write_to_iceberg(self, df: DataFrame, database: str, table: str,
                         mode: str = "append") -> None:
        """
        Write DataFrame to an Iceberg table.

        IMPORTANT: Write operations use IAM permissions, NOT Lake Formation grants.
        Ensure the job's IAM role has:
        - s3:PutObject on the table's S3 location
        - glue:UpdateTable (for schema evolution)

        Args:
            df: DataFrame to write
            database: Target database
            table: Target table
            mode: Write mode - "append", "overwrite", or "merge"
        """
        full_table = f"`{database}`.`{table}`"

        if mode == "append":
            logger.info(f"Appending to Iceberg table: {full_table}")
            df.writeTo(full_table).append()

        elif mode == "overwrite":
            logger.info(f"Overwriting Iceberg table: {full_table}")
            df.writeTo(full_table).overwritePartitions()

        else:
            raise ValueError(f"Unsupported write mode: {mode}. Use 'append' or 'overwrite'")

        logger.info(f"Successfully wrote to {full_table}")

    def merge_into_iceberg(self, source_df: DataFrame, database: str, table: str,
                           merge_key: str,
                           update_columns: list = None) -> None:
        """
        Perform MERGE INTO (upsert) operation on an Iceberg table.

        This is useful for CDC patterns where you need to:
        - Insert new records
        - Update existing records
        - Optionally delete records

        Args:
            source_df: Source DataFrame with changes
            database: Target database
            table: Target table
            merge_key: Column(s) to match on (e.g., "id" or "id, version")
            update_columns: Columns to update (None = all columns)
        """
        # Create temp view for the source data
        temp_view = f"_merge_source_{table}"
        source_df.createOrReplaceTempView(temp_view)

        target_table = f"`{database}`.`{table}`"

        # Build UPDATE SET clause
        if update_columns:
            update_set = ", ".join([f"t.`{c}` = s.`{c}`" for c in update_columns])
        else:
            # Update all columns from source
            update_set = ", ".join([f"t.`{c}` = s.`{c}`" for c in source_df.columns])

        # Build INSERT columns and values
        insert_cols = ", ".join([f"`{c}`" for c in source_df.columns])
        insert_vals = ", ".join([f"s.`{c}`" for c in source_df.columns])

        merge_sql = f"""
            MERGE INTO {target_table} t
            USING {temp_view} s
            ON t.{merge_key} = s.{merge_key}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        logger.info(f"Executing MERGE INTO {target_table}")
        self.spark.sql(merge_sql)
        logger.info("MERGE completed successfully")

    # =========================================================================
    # ICEBERG-SPECIFIC OPERATIONS (FGAC-compatible)
    # =========================================================================

    def get_iceberg_snapshots(self, database: str, table: str) -> DataFrame:
        """
        Get Iceberg table snapshots (supported with FGAC).

        Supported metadata tables with FGAC:
        - history, metadata_log_entries, snapshots, files, manifests, refs

        Hidden columns (not accessible): partitions, path, summaries
        """
        return self.spark.sql(f"SELECT * FROM `{database}`.`{table}`.snapshots")

    def get_iceberg_history(self, database: str, table: str) -> DataFrame:
        """Get Iceberg table history."""
        return self.spark.sql(f"SELECT * FROM `{database}`.`{table}`.history")

    def time_travel_query(self, database: str, table: str,
                          snapshot_id: int = None,
                          timestamp: datetime = None) -> DataFrame:
        """
        Query historical data using Iceberg time travel.

        Args:
            database: Database name
            table: Table name
            snapshot_id: Specific snapshot ID to query
            timestamp: Query as of this timestamp
        """
        full_table = f"`{database}`.`{table}`"

        if snapshot_id:
            logger.info(f"Time travel query to snapshot {snapshot_id}")
            return self.spark.sql(
                f"SELECT * FROM {full_table} VERSION AS OF {snapshot_id}"
            )
        elif timestamp:
            ts_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Time travel query to timestamp {ts_str}")
            return self.spark.sql(
                f"SELECT * FROM {full_table} TIMESTAMP AS OF '{ts_str}'"
            )
        else:
            raise ValueError("Must provide either snapshot_id or timestamp")

    def expire_snapshots(self, database: str, table: str,
                         retain_days: int = 7) -> None:
        """
        Expire old snapshots to reclaim storage.

        NOTE: This requires s3:DeleteObject permission via IAM.
        """
        logger.info(f"Expiring snapshots older than {retain_days} days")
        self.spark.sql(f"""
            CALL spark_catalog.system.expire_snapshots(
                table => '{database}.{table}',
                older_than => TIMESTAMP '{datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}' - INTERVAL {retain_days} DAYS,
                retain_last => 1
            )
        """)

    def compact_data_files(self, database: str, table: str) -> None:
        """
        Compact small data files to improve query performance.

        NOTE: This requires s3:PutObject and s3:DeleteObject via IAM.
        """
        logger.info(f"Compacting data files for {database}.{table}")
        self.spark.sql(f"""
            CALL spark_catalog.system.rewrite_data_files(
                table => '{database}.{table}',
                options => map('target-file-size-bytes', '134217728')
            )
        """)


def create_sample_schema() -> StructType:
    """Create a sample schema for demonstration."""
    return StructType([
        StructField("id", LongType(), False),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_data", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("year", StringType(), True),
        StructField("month", StringType(), True),
        StructField("day", StringType(), True),
    ])


def transform_data(df: DataFrame) -> DataFrame:
    """
    Apply transformations to the data.

    All transformations use DataFrame API (FGAC-compliant).
    Do NOT convert to RDD!
    """
    return (
        df
        # Add processing timestamp
        .withColumn("processed_at", current_timestamp())

        # Add partition columns for Iceberg
        .withColumn("year", year(col("event_timestamp")).cast("string"))
        .withColumn("month", month(col("event_timestamp")).cast("string"))
        .withColumn("day", dayofmonth(col("event_timestamp")).cast("string"))

        # Example: Clean null values
        .withColumn("event_type", coalesce(col("event_type"), lit("unknown")))

        # Example: Add row hash for deduplication
        .withColumn("row_hash", spark_hash(col("id"), col("event_timestamp")))
    )


def main():
    """Main ETL entry point."""

    # Parse job arguments
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "source_database",
        "source_table",
        "target_database",
        "target_table",
        "warehouse_path"
    ])

    # Initialize Spark and Glue context
    # NOTE: Only ONE SparkSession is allowed with FGAC
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger.info("=" * 60)
    logger.info("Starting Lake Formation + Iceberg ETL Job")
    logger.info("=" * 60)

    try:
        # Create ETL processor
        etl = LakeFormationIcebergETL(spark, glue_context)

        # Extract parameters
        source_db = args["source_database"]
        source_table = args["source_table"]
        target_db = args["target_database"]
        target_table = args["target_table"]
        warehouse_path = args["warehouse_path"]

        # =====================================================================
        # STEP 1: Read from Lake Formation protected source
        # (FGAC row/column filters are automatically applied)
        # =====================================================================
        logger.info(f"Reading from source: {source_db}.{source_table}")
        source_df = etl.read_table(source_db, source_table)

        # Log what columns we can see (may be filtered by FGAC)
        logger.info(f"Visible columns: {source_df.columns}")
        record_count = source_df.count()
        logger.info(f"Records read (after FGAC filtering): {record_count}")

        if record_count == 0:
            logger.warning("No records to process - exiting")
            job.commit()
            return

        # =====================================================================
        # STEP 2: Transform data using DataFrame API (no RDDs!)
        # =====================================================================
        logger.info("Applying transformations")
        transformed_df = transform_data(source_df)

        # =====================================================================
        # STEP 3: Create target Iceberg table if it doesn't exist
        # (Uses IAM permissions, not Lake Formation)
        # =====================================================================
        if not etl.table_exists(target_db, target_table):
            logger.info(f"Creating target Iceberg table: {target_db}.{target_table}")
            etl.create_iceberg_table(
                database=target_db,
                table=target_table,
                schema=create_sample_schema(),
                warehouse_path=warehouse_path,
                partition_columns=["year", "month", "day"],
                comment="Events processed by Lake Formation FGAC ETL"
            )

        # =====================================================================
        # STEP 4: Write to Iceberg table
        # (Uses IAM permissions for S3 write)
        # =====================================================================
        logger.info(f"Writing to target: {target_db}.{target_table}")

        # Option A: Simple append
        etl.write_to_iceberg(transformed_df, target_db, target_table, mode="append")

        # Option B: MERGE for upsert (uncomment if needed)
        # etl.merge_into_iceberg(
        #     source_df=transformed_df,
        #     database=target_db,
        #     table=target_table,
        #     merge_key="id"
        # )

        # =====================================================================
        # STEP 5: Optional - Table maintenance
        # =====================================================================
        # Uncomment these for production jobs:

        # Expire old snapshots (keep last 7 days)
        # etl.expire_snapshots(target_db, target_table, retain_days=7)

        # Compact small files
        # etl.compact_data_files(target_db, target_table)

        # =====================================================================
        # STEP 6: Log results
        # =====================================================================
        logger.info("=" * 60)
        logger.info("ETL Job Completed Successfully")
        logger.info(f"Records processed: {record_count}")
        logger.info(f"Target table: {target_db}.{target_table}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise

    finally:
        job.commit()


if __name__ == "__main__":
    main()
