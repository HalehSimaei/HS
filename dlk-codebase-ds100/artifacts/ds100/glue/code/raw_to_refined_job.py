### RAW to REFINED Job for Helpscout Customers
### This script extracts customer data from Helpscout RAW ZONE, transforms it, and loads it into an Iceberg table in the REFINED ZONE.
import sys
import json
import time
import logging
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.utils import AnalysisException
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError


class RawtoRefinedHelpscout:
    ### This class handles the transformation and loading of Helpscout data from raw to refined layer.
    ### It uses AWS Glue and Apache Iceberg for data processing and storage.
    def __init__(self):
        ### Get script arguments from glue job
        self.args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "env",
                "data_source",
                "from_layer",
                "to_layer",
                "confidentiality_level",
                "account_id",
                "aws_catalog_name",
                "region",
            ],
        )

        ### Initialize variables from arguments
        self.job_name = self.args["JOB_NAME"]
        self.environment = self.args["env"]
        self.data_source = self.args["data_source"]
        self.from_layer = self.args["from_layer"]
        self.to_layer = self.args["to_layer"]
        self.confidentiality_level = self.args["confidentiality_level"]
        self.account_id = self.args["account_id"]
        self.aws_catalog_name = self.args["aws_catalog_name"]
        self.region = self.args["region"]

        self.source_database_name = f"stg-dlk-{self.environment}-{self.data_source}-{self.from_layer}-db"
        self.destination_database_name = f"stg-dlk-{self.environment}-{self.confidentiality_level}-{self.to_layer}-db"
        self.source_bucket = f"stg-dlk-{self.environment}-{self.data_source}-{self.from_layer}"
        self.destination_bucket = f"stg-dlk-{self.environment}-{self.confidentiality_level}-{self.to_layer}"

        ### Spark Session
        self.spark = SparkSession.builder \
            .appName(self.job_name) \
            .config(f"spark.sql.catalog.{self.aws_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{self.aws_catalog_name}.warehouse", f"s3://{self.destination_bucket}") \
            .config(f"spark.sql.catalog.{self.aws_catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config(f"spark.sql.catalog.{self.aws_catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config(f"spark.sql.catalog.{self.aws_catalog_name}.glue.lakeformation-enabled", "true") \
            .config(f"spark.sql.catalog.{self.aws_catalog_name}.glue.skip-name-validation", "true") \
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
            .config(f"spark.sql.catalog.{self.aws_catalog_name}.glue.id", f"{self.account_id}") \
            .config("spark.sql.catalogImplementation", "in-memory") \
            .config("spark.sql.defaultCatalog", self.aws_catalog_name) \
            .getOrCreate()
        
        ### Glue Context
        self.context = GlueContext(self.spark)

        ### logger initialization
        self.logger = logging.getLogger()
        self.logger = self.context.get_logger()

        ### Initialize the job
        self.job = Job(self.context)
        self.job.init(self.job_name, self.args)

        ### Initialize the S3 client
        self.s3_client = boto3.client("s3", region_name=self.region)

        ### Table configurations for the job
        self.table_configs = {
            "customers": {
                "primary_keys": ["customer_id"],
                "condition": "old.customer_id = new.customer_id",
            }
        }
#### get the existing record count from the refined table before extract
    def get_existing_record_count(self, key: str) -> int:
        try:
            response = self.s3_client.get_object(Bucket=self.source_bucket, Key=key)
            existing_df = pd.read_parquet(BytesIO(response["Body"].read()), engine="pyarrow")
            return existing_df.shape[0]
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                self.logger.warning("No existing file found in raw bucket.")
                return 0
            self.logger.exception("Error occurred while accessing S3.")
            raise

#### read the customer tables from the raw catalog db
    def read_tables_from_catalog(self, table_name: str) -> DataFrame:
        try:
            full_table_name = f"`AwsDataCatalog`.`{self.source_database_name}`.`{table_name}`"
            self.logger.info(f"Loading table: {full_table_name}")
            df = self.spark.sql(f"SELECT * FROM {full_table_name}")
            self.logger.info(f"Loaded {df.count()} records")
            if "updated_on" not in df.columns:
                df = df.withColumn("updated_on", F.current_timestamp())
            return df
        except AnalysisException as e:
            self.logger.error(f"Analysis error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return None

##### extract the fields from the content column
    def extract_from_content(self, df: DataFrame, field: str, alias: str, length: int = 100) -> DataFrame:
        schema = StructType([StructField(field, StringType(), True)])
        df = df.withColumn("content_json", F.from_json(F.col("content"), schema))
        return df.withColumn(alias, F.expr(f"nvl(left(trim(content_json.{field}), {length}), '')"))

#### extract the email field from the content column
    def extract_email(self, df: DataFrame) -> DataFrame:
        schema = StructType([StructField("emails", ArrayType(StructType([StructField("value", StringType(), True)])), True)])
        df = df.withColumn("content_parsed", F.from_json(F.col("content"), schema))
        return df.withColumn(
            "email",
            F.expr("""
                nvl(
                    left(
                        trim(
                            concat_ws(',', array_distinct(
                                filter(transform(content_parsed.emails, x -> trim(coalesce(x.value, ''))), x -> x != '')
                            ))
                        ), 100
                    ),
                    ''
                )
            """)
        )

#### extract the address fields from the content column - to further extract country, state, city, postal code.
    def extract_address_field(self, df: DataFrame, field_name: str, alias: str, length: int = 80) -> DataFrame:
        schema = StructType([
            StructField("_embedded", StructType([
                StructField("address", StructType([
                    StructField(field_name, StringType(), True)
                ]), True)
            ]), True)
        ])
        df = df.withColumn("content_embedded", F.from_json(F.col("content"), schema))
        return df.withColumn(alias, F.expr(f"nvl(left(trim(content_embedded._embedded.address.{field_name}), {length}), '')"))

#### extract the full name from the content column
    def extract_full_name(self, df: DataFrame) -> DataFrame:
        schema = StructType([
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
        ])
        df = df.withColumn("content_json", F.from_json(F.col("content"), schema))
        return df.withColumn(
            "full_name",
            F.expr("""
                nvl(left(trim(concat(content_json.firstName, ' ', content_json.lastName)), 100), '')
            """)
        )

#### load the data to iceberg table in the refined catalog db
    def load_to_iceberg(self, df: DataFrame, table_name: str, condition: str):
        full_table_name = f"AwsDataCatalog.`{self.destination_database_name}`.`{table_name}`"
        load_path = f"s3://{self.destination_bucket}/{table_name}"
        try:
            if self.spark.catalog.tableExists(f"{self.aws_catalog_name}.{self.destination_database_name}.{table_name}"):
                self.logger.info(f"Overwriting existing Iceberg table: {full_table_name}")
                (
                    df.writeTo(full_table_name)
                    .using("iceberg") # using Iceberg format
                    .tableProperty("location", load_path) # location of the Refined table in S3
                    .tableProperty("write.format.default", "parquet") # default format for the table
                    .tableProperty("write.compression", "snappy") # compression type
                    .overwriteFiles() # overwrite existing files in the table
                )
            else:
                self.logger.info(f"Creating new Iceberg table: {full_table_name}")
                (
                    df.writeTo(full_table_name)
                    .using("iceberg")
                    .tableProperty("location", load_path)
                    .tableProperty("write.format.default", "parquet")
                    .tableProperty("write.compression", "snappy")
                    .tableProperty("target-file-size-bytes", "268435456") # target size - 256 MB
                    .create()
                )
        except Exception as e:
            self.logger.error(f"Error loading table {table_name}: {str(e)}")
            sys.exit(1)

#### get the customers record count in refined table after extract.
    def get_record_count_after_extract(self, key: str) -> int:
        try:
            response = self.s3_client.get_object(Bucket=self.source_bucket, Key=key)
            postextract_df = pd.read_parquet(BytesIO(response["Body"].read()), engine="pyarrow")
            return postextract_df.shape[0]
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                self.logger.warning("No existing file found in raw bucket.")
                return 0
            self.logger.exception("Error occurred while accessing S3.")
            raise

#### MAIN JOB PROCESS -- This function orchestrates the entire ETL process.
    def runJob(self):
        self.logger.info("Starting the raw to refined job for Helpscout Customers...")

        ### Step 1: Get existing customer count from refined table
        s3_key = "helpscout/Customers/Customers_Info.parquet"
        try:
            count_before_extract = self.get_existing_record_count(s3_key)
            self.logger.info(f"Expected at least 1000 records in RAW Customers Table, {count_before_extract} were found.")
        except Exception:
            self.logger.error("Failed to get existing records.")

        ### Step 2: Read the customers table data from the Raw catalog
        for table_name, config in self.table_configs.items():
            df = self.read_tables_from_catalog(table_name)
            if df is None:
                continue
            
        ### Step 3: Transform the data - get specific columns from the Raw catalog.
            df = df.select("customer_key", "customer_id", "created", "updated", "content", "updated_on")
            df = self.extract_email(df)
            df = self.extract_from_content(df, "firstName", "first_name")
            df = self.extract_from_content(df, "lastName", "last_name")
            df = self.extract_full_name(df)
            df = self.extract_from_content(df, "location", "location", length=45)
            df = self.extract_address_field(df, "country", "country_code", length=45)
            df = self.extract_address_field(df, "state", "state", length=45)
            df = self.extract_address_field(df, "city", "city", length=80)
            df = self.extract_address_field(df, "postalCode", "postal_code", length=45)

            df = df.drop("content").orderBy("customer_id", "customer_key")

        ### Step 4: Load the transformed data to the Iceberg table in the refined catalog
            self.load_to_iceberg(df, table_name, config["condition"])

        ### Step 5: Data Validation - row count after extracting from Raw table
        try:
            count_after_extract = self.get_record_count_after_extract(s3_key)
            self.logger.info(f"Customers record count after extraction: {count_after_extract}")
        except Exception as e:
            self.logger.error("Failed to get records count.")
            raise e 

        ### Step 6: Data Validation - check count results
        nbInsRecords = count_after_extract - count_before_extract

        if count_after_extract >= 100:
            self.logger.info(f"Expected at least 1000 records in RAW Customers Table, {nbInsRecords} were found. - ETL Success")

        if nbInsRecords > 0:
            self.logger.info(f"{nbInsRecords} new customers were inserted into Refined Bucket.")
        elif nbInsRecords == 0:
            self.logger.warning("No new data was inserted into Refined Bucket.")
        else:
            self.logger.warning(f"{abs(nbInsRecords)} customers were deleted from Refined Bucket.")
        

        self.job.commit()


if __name__ == "__main__":
    RawtoRefinedHelpscout().runJob()

