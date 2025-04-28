# Testing
import sys
import time
import json
import logging
import urllib3
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from abc import ABC, abstractmethod
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException, ParseException
import requests
import urllib.parse
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Logging setup
# check test
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
http = urllib3.PoolManager()

# ----------------------
# Deduplicate function
# ----------------------
def deduplicate_latest(df: DataFrame, key_col: str, order_col: str, method: str = "row_number") -> DataFrame:
    """
    Deduplicate a DataFrame by keeping the latest record per key based on an order column.
    """
    if key_col not in df.columns or order_col not in df.columns:
        raise ValueError(f"Missing required columns: '{key_col}' or '{order_col}'")

    window_spec = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())

    if method == "row_number":
        ranked_df = df.withColumn("rank", F.row_number().over(window_spec))
        deduped_df = ranked_df.filter(F.col("rank") == 1).drop("rank")
    elif method == "dense_rank":
        ranked_df = df.withColumn("rank", F.dense_rank().over(window_spec))
        deduped_df = ranked_df.filter(F.col("rank") == 1).drop("rank")
    else:
        raise ValueError("Invalid method. Choose 'row_number' or 'dense_rank'.")

    return deduped_df

# ----------------------
# Classes
# ----------------------

class SparkSessionManager:
    def __init__(self, job_name, bucket, account_id, aws_catalog_name):
        self.spark = SparkSession.builder \
            .appName(job_name) \
            .enableHiveSupport() \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config(f"spark.sql.catalog.{aws_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{aws_catalog_name}.warehouse", f"s3://{bucket}") \
            .config(f"spark.sql.catalog.{aws_catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config(f"spark.sql.catalog.{aws_catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config(f"spark.sql.catalog.{aws_catalog_name}.glue.lakeformation-enabled", "true") \
            .config(f"spark.sql.catalog.{aws_catalog_name}.glue.skip-name-validation", "true") \
            .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
            .config(f"spark.sql.catalog.{aws_catalog_name}.glue.id", f"{account_id}") \
            .config("spark.sql.defaultCatalog", aws_catalog_name) \
            .getOrCreate()
        self.glue_context = GlueContext(self.spark)
        self.logger = self.glue_context.get_logger()

class HTTPIngest:
    def __init__(self, api_base_url, app_id, app_secret):
        self.api_base_url = api_base_url
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = None
        self.token_expiry_time = 0

    def get_access_token(self):
        if self.access_token and time.time() < self.token_expiry_time:
            return self.access_token

        response = requests.post(
            f"{self.api_base_url}oauth2/token",
            data={
                'grant_type': 'client_credentials',
                'client_id': self.app_id,
                'client_secret': self.app_secret
            }
        )

        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            self.token_expiry_time = time.time() + token_data.get("expires_in", 0)
            return self.access_token
        else:
            raise Exception("Failed to obtain access token")

    def fetch_data_with_backoff(self, url, headers, max_retries=3, initial_wait=3):
        retry_count = 0
        wait_time = initial_wait

        while retry_count < max_retries:
            try:
                response = http.request("GET", url, headers=headers)
                if 200 <= response.status < 300:
                    return response
                else:
                    logging.error(f"HTTP {response.status}: {response.data.decode('utf-8')}")
            except Exception as e:
                logging.exception(f"HTTP request failed: {e}")

            retry_count += 1
            time.sleep(wait_time)
            wait_time *= 2

        raise Exception("Max retries exceeded")

class FetchBase(ABC):
    def __init__(self, http_client: HTTPIngest):
        self.http_client = http_client

    @abstractmethod
    def fetch_from_API(self):
        pass

class HelpScoutFetcher(FetchBase):
    def __init__(self, http_client: HTTPIngest, since_timestamp: str = None):
        super().__init__(http_client)
        self.since_timestamp = since_timestamp

    def fetch_from_API(self):
        token = self.http_client.get_access_token()
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        customers, page = [], 1

        while True:
            url = f"{self.http_client.api_base_url}customers?page={page}"
            if self.since_timestamp:
                url += f"&modifiedSince={urllib.parse.quote(self.since_timestamp)}"
                logging.info(f"Final API URL: {url}")
            response = self.http_client.fetch_data_with_backoff(url, headers)
            data = json.loads(response.data)
            extracted = [
                {
                    "customer_id": c["id"],
                    "created": c["createdAt"],
                    "updated": c["updatedAt"],
                    "Content": json.dumps(c)
                }
                for c in data.get('_embedded', {}).get('customers', [])
            ]
            customers.extend(extracted)

            total_pages = data.get("page", {}).get("totalPages", 1)
            if page >= total_pages or not extracted:
                break
            page += 1

        return customers

class SSMTimestampManager:
    def __init__(self, parameter_name):
        self.client = boto3.client('ssm')
        self.parameter_name = parameter_name

    def get_last_timestamp(self):
        try:
            response = self.client.get_parameter(Name=self.parameter_name, WithDecryption=True)
            value = response['Parameter']['Value']
            logging.info(f"Last timestamp from SSM: {value}")
            return value
        except self.client.exceptions.ParameterNotFound:
            logging.warning(f"SSM parameter {self.parameter_name} not found.")
            return None

    def update_timestamp(self, new_timestamp):
        try:
            self.client.put_parameter(
                Name=self.parameter_name,
                Value=new_timestamp,
                Type='String',
                Overwrite=True
            )
            logging.info(f"Successfully updated SSM parameter {self.parameter_name} with timestamp {new_timestamp}")
        except ClientError as e:
            logging.error(f"Failed to update SSM parameter: {str(e)}")
            raise

class IcebergWriter:
    def __init__(self, spark: SparkSession, catalog: str, database: str, destination_bucket: str):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.destination_bucket = destination_bucket

    def write(self, df: DataFrame, table_name: str, condition: str):
        temp_table = "temp_table"
        df.createOrReplaceTempView(temp_table)
        sql_table_name = f"`{self.catalog}`.`{self.database}`.`{table_name}`"

        try:
            df.printSchema()
            df.show(5, truncate=False)

            null_count = df.filter("customer_id IS NULL").count()
            if null_count > 0:
                logging.warning(f"Found {null_count} rows with null customer_id. These rows will be skipped.")
                df = df.filter("customer_id IS NOT NULL")
                df.createOrReplaceTempView(temp_table)

            logging.info(f"Schema of Iceberg table `{sql_table_name}`:")
            self.spark.sql(f"DESCRIBE TABLE {sql_table_name}").show(truncate=False)

            merge_sql = f"""
                MERGE INTO {sql_table_name} old
                USING {temp_table} new
                ON {condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """
            logging.info("Running Iceberg MERGE statement...")
            self.spark.sql(merge_sql)

        except Exception as e:
            import traceback
            logging.error(f"Error writing to Iceberg table {table_name}: {str(e)}")
            traceback.print_exc()
            sys.exit(1)

class HelpScoutETLJob:
    def __init__(self, args):
        self.args = args
        self.ssm = SSMTimestampManager(args['ssm_param_name'])
        self.last_timestamp = self.ssm.get_last_timestamp()

        self.http_client = HTTPIngest(args['helpscout_api_base_url'], args['helpscout_app_id'], args['helpscout_app_secret'])
        self.fetcher = HelpScoutFetcher(self.http_client, self.last_timestamp)
        self.spark_mgr = SparkSessionManager(args['JOB_NAME'], args['bucket_name'], args['account_id'], args['aws_catalog_name'])
        self.writer = IcebergWriter(self.spark_mgr.spark, args['aws_catalog_name'], args['db_name'], args['bucket_name'])

        self.glue_context = self.spark_mgr.glue_context
        self.job = Job(self.glue_context)
        self.job.init(args['JOB_NAME'], args)

    def run(self):
        customers = self.fetcher.fetch_from_API()
        if not customers:
            logging.info("No new data to process.")
            return

        df = pd.DataFrame(customers)
        df['created'] = pd.to_datetime(df['created'], errors='coerce')
        df = df[df['created'] >= pd.Timestamp("1970-01-01", tz='UTC')]
        df['Updated_On'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce').astype('Int64')

        spark_df = self.spark_mgr.spark.createDataFrame(df)
        spark_df = spark_df.withColumn("customer_id", col("customer_id").cast("long"))

        # Deduplicate
        spark_df = deduplicate_latest(spark_df, key_col="customer_id", order_col="updated", method="row_number")

        if spark_df.rdd.isEmpty():
            logging.info("No deduplicated records to write. Exiting.")
            return

        logging.info("Spark DataFrame Schema After Deduplication:")
        spark_df.printSchema()
        spark_df.show(5, truncate=False)

        self.writer.write(spark_df, "helpscout_customers", "old.customer_id = new.customer_id")

        # Update SSM Parameter with Max updatedAt
        latest_ts = df['updated'].max()
        if pd.notna(latest_ts):
            formatted_ts = latest_ts.strftime('%Y-%m-%dT%H:%M:%SZ')
            self.ssm.update_timestamp(formatted_ts)

        self.job.commit()

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'helpscout_api_base_url', 'helpscout_app_id', 'helpscout_app_secret',
        'bucket_name', 'account_id', 'aws_catalog_name', 'db_name', 'ssm_param_name'
    ])
    job = HelpScoutETLJob(args)
    job.run()

    logging.info("ETL job completed successfully.")
