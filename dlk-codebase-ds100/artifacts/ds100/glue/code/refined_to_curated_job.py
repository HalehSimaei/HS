import sys
from pyspark import SparkContext, SparkConf
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


class RefinedToCuratedCAMDB:
    """
    Handles refined to curated transformations and upserts for CAMDB.
    """

    def __init__(self):
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
            ],
        )
        self.job_name = self.args["JOB_NAME"]
        self.environment = self.args["env"]
        self.data_source = self.args["data_source"]
        self.from_layer = self.args["from_layer"]
        self.to_layer = self.args["to_layer"]
        self.account_id = self.args["account_id"]
        self.confidentiality_level = self.args["confidentiality_level"]
        self.aws_catalog_name = self.args["aws_catalog_name"]
        self.source_database_name = (
            f"stg-dlk-{self.environment}-{self.data_source}-{self.from_layer}-db"
        )
        self.destination_database_name = (
            f"stg-dlk-{self.environment}-{self.confidentiality_level}-{self.to_layer}-db"
        )
        self.source_bucket = (
            f"stg-dlk-{self.environment}-{self.data_source}-{self.from_layer}"
        )
        self.destination_bucket = (
            f"stg-dlk-{self.environment}-{self.confidentiality_level}-{self.to_layer}"
        )

        # Initialize Spark and Glue contexts
        spark = SparkSession.builder.getOrCreate()
        self.context = GlueContext(spark)
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
            .config("hive.metastore.warehouse.dir", "") \
            .config("spark.sql.defaultCatalog", self.aws_catalog_name) \
            .getOrCreate()

        self.logger = self.context.get_logger()
        self.job = Job(self.context)
        self.job.init(self.job_name, self.args)

        # Define table configurations
        self.table_configs = {
            "closedrestorationorderexporter": {
                "primary_keys": ["sapid"],
                "condition": "old.sapid = new.sapid",
            },
            # "openrestorationorderexporter": {
            #     "primary_keys": ["sapid", "last_event_date"],
            #     "condition": "old.sapid = new.sapid AND old.last_event_date = new.last_event_date",
            # },
        }

    def read_tables_from_catalog(self, table_name: str) -> DataFrame:
        try:
            full_table_name = f"`AwsDataCatalog`.`{self.source_database_name}`.`{table_name}`"
            self.logger.info(f"Loading Iceberg table: {full_table_name}")
            
            # Load the full table
            df = self.spark.sql(f"SELECT * FROM {full_table_name}")
            
            # Get the latest ingest date
            max_ingest_date = df.select(F.max(F.date_format("ingest_timestamp", "yyyy-MM-dd"))).collect()[0][0]
            if max_ingest_date is None:
                self.logger.info("No data found in the table.")
                return None
            
            self.logger.info(f"Filtering data for the latest ingest_date: {max_ingest_date}")
            
            # Filter the DataFrame for rows with the latest ingest date
            df = df.filter(F.date_format("ingest_timestamp", "yyyy-MM-dd") == max_ingest_date)
            
            return df
        except AnalysisException as e:
            self.logger.error(f"Error loading table {full_table_name}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            return None

    def upsert_to_iceberg(self, df: DataFrame, table_name: str, condition: str) -> None:
        """
        Upsert the Spark DataFrame into an Iceberg table in curated zone.

        Params:
            - df (DataFrame): the Spark DataFrame to be written
            - table_name (str): the name of the target table
            - condition (str): the condition to control the upsert operation
        """
        temp_table = "temp_table"
        df.createOrReplaceTempView(temp_table)

        load_path = f"s3://{self.destination_bucket}/{table_name}"
        curated_full_table_name = f"AwsDataCatalog.`{self.destination_database_name}`.`{table_name}`"

        try:
            # Merge new data if the table exists
            self.spark.sql(
                f"""
                MERGE INTO {curated_full_table_name} old
                USING {temp_table} new
                ON {condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        except AnalysisException:
            # Create the table if it doesn't exist
            (
                df.writeTo(curated_full_table_name)
                .using("iceberg")
                .tableProperty("location", load_path)
                .tableProperty("write.format.default", "parquet")
                .tableProperty("target-file-size-bytes", "268435456")  # 256 MB
                .tableProperty("write.compression", "snappy")
                .create()
            )
        except Exception as e:
            self.logger.error(f"Error writing table Iceberg {table_name}: {str(e)}")
            sys.exit(1)

    def runJob(self):
        for table_name, config in self.table_configs.items():
            self.logger.info(f"Processing table: {table_name}")
            glue_table = self.read_tables_from_catalog(table_name)

            if not glue_table:
                self.logger.info(f"No data available for table: {table_name}")
                continue

            # Upsert the transformed data to the destination table
            self.upsert_to_iceberg(
                df=glue_table,
                table_name=table_name,
                condition=config["condition"],
            )

        self.job.commit()


if __name__ == "__main__":
    pipeline = RefinedToCuratedCAMDB()
    pipeline.runJob()
