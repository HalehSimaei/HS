import json
import time
import logging
import urllib3
import boto3
import sys
import pandas as pd
from io import BytesIO
from abc import ABC, abstractmethod
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from datetime import datetime
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global HTTP manager
http = urllib3.PoolManager()

# 1. HTTPIngest class: contains access token and fetch data with backoff logic
class HTTPIngest:
    access_token = None
    token_expiry_time = 0

    def __init__(self, api_base_url, app_id, app_secret):
        self.api_base_url = api_base_url
        self.app_id = app_id
        self.app_secret = app_secret

    def get_access_token(self):
        if self.access_token and time.time() < self.token_expiry_time:
            remaining = self.token_expiry_time - time.time()
            logging.info(f"Using cached access token (expires in {remaining:.0f} seconds)")
            return self.access_token

        logging.info("Fetching new access token using Client Credentials Flow")
        response = requests.post(
            f"{self.api_base_url}oauth2/token",
            data={
                'grant_type': 'client_credentials',
                'client_id': self.app_id,
                'client_secret': self.app_secret
            }
        )
        logging.info("Successfully fetched new access token")

        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            self.token_expiry_time = time.time() + token_data.get("expires_in", 0)
            return self.access_token
        else:
            logging.error("Failed to obtain access token")
            raise Exception("Failed to obtain access token")

    def fetch_data_with_backoff(self, url, headers, max_retries=3, initial_wait=3):
        retry_count = 0
        wait_time = initial_wait
        last_response = ""

        while retry_count < max_retries:
            try:
                response = http.request("GET", url, headers=headers)
                if 200 <= response.status < 300:
                    return response
                else:
                    last_response = response.data.decode('utf-8')
                    logging.error(f"HTTP {response.status}: {last_response}")
            except Exception as e:
                logging.exception(f"HTTP request failed: {e}")
            retry_count += 1
            time.sleep(wait_time)
            wait_time *= 2

        raise Exception(f"Max retries exceeded. Last response: {last_response}")

# 2. Abstract FetchBase class
class FetchBase(ABC):
    def __init__(self, http_client: HTTPIngest):
        self.http_client = http_client

    @abstractmethod
    def fetch_from_API(self):
        pass

# 3. Concrete fetcher
class Fetch(FetchBase): #uses  HTTP Client class
    def fetch_from_API(self):
        token = self.http_client.get_access_token()
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        customers = []
        page = 1

        while True:
            url = f"{self.http_client.api_base_url}customers?page={page}"
            if since_timestamp:
                url += f"&modifiedSince={since_timestamp}"
            response = self.http_client.fetch_data_with_backoff(url, headers)
            data = json.loads(response.data)

            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            if page == 1:
                total_elements = page_info.get("totalElements", 0)
                page_size = page_info.get("size", 0)
                logging.info(f"Total elements: {total_elements}, Total pages: {total_pages}, Page size: {page_size}")

            if total_elements > 0:
                logging.info(f"Helpscout contains {total_elements} records")
            else:
                is_success = False
                logging.error("Helpscout does not contain any data")
                break

            extracted = [
                {
                    "customer_id": c.get("id"),
                    "created": c.get("createdAt"),
                    "updated": c.get("updatedAt"),
                    "Content": json.dumps(c)
                } for c in data.get('_embedded', {}).get('customers', [])
            ]
            customers.extend(extracted)

            logging.info(f"Fetched page {page}/{total_pages}, Records so far: {len(customers)}")

            if page >= total_pages:
                break
            page += 1

        return customers , total_elements
    
def upsert_to_iceberg(spark, df, table_name, condition):
    temp_table = "temp_table"
    df.createOrReplaceTempView(temp_table)
    curated_full_table_name = f"AwsDataCatalog.curated_db.{table_name}"

    try:
        spark.sql(f"""
            MERGE INTO {curated_full_table_name} t
            USING {temp_table} s
            ON {condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    except AnalysisException:
        df.writeTo(curated_full_table_name) \
            .using("iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.compression", "snappy") \
            .create()


# 4. Save logic
##class ParquetSaver:
    ##@staticmethod
    ##def save_to_parquet(customers, start_key=1, compression="gzip", compression_level=5):
        ##df = pd.DataFrame(customers)
        ##df['created'] = pd.to_datetime(df['created'], errors='coerce')
        ##df = df[df['created'] >= pd.Timestamp("1970-01-01", tz='UTC')]
        ##df = df.sort_values(by='created')
        ##df['Customer_Key'] = range(start_key, start_key + len(df))
        ##df['Updated_On'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        ##if df['customer_id'].duplicated().any():
            ##logging.warning("Duplicate customer_id found.")
        ##else:
            ##logging.info("All customer_id values are unique.")

        ##df = df[['Customer_Key', 'customer_id', 'created', 'updated', 'Content', 'Updated_On']]
        ##buffer = BytesIO()
        ##df.to_parquet(buffer, index=False, engine="pyarrow", compression=compression, compression_level=compression_level)
        ##buffer.seek(0)
        ##return buffer

# 5. Job runner
class RunJob:
    def __init__(self, args):
        self.bucket = args['bucket_name']
        self.region = args['region_name']
        self.key = args['S3_key']

        self.http_client = HTTPIngest(args['helpscout_api_base_url'], args['helpscout_app_id'], args['helpscout_app_secret'])
        self.fetcher = Fetch(self.http_client)
        self.saver = ParquetSaver()
        self.s3 = boto3.client("s3", region_name=self.region)

    def run(self):
        logging.info("Starting Source to Raw job")

        customers, total_elements = self.fetcher.fetch_from_API()
        if not customers:
            logging.info("No customers fetched.")
            return
        
            # Upsert the transformed data to the destination table
            self.upsert_to_iceberg(
                df=glue_table,
                table_name=table_name,
                condition=config["condition"],
            )
        
        latest_timestamp = max([c["updated"] for c in customers])

        buffer = self.saver.save_to_parquet(customers)
        self.s3.put_object(Body=buffer.getvalue(), Bucket=self.bucket, Key=self.key)
        logging.info(f"Saved customers data to S3: {self.key}")
        logging.info(f"Source to RAW Ingest success: {total_elements} records ingested.")

# Main 
if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['bucket_name', 'region_name', 'S3_key' , 'helpscout_app_id', 'helpscout_app_secret', 'helpscout_api_base_url'])
    job = RunJob(args)
    job.run()


# 3. Job runner
class RunJob:
    def __init__(self, args):
        self.bucket = args['bucket_name']
        self.region = args['region_name']
        self.ssm_param = args['ssm_param_name']
        self.catalog_name = "AwsDataCatalog"
        self.database_name = "curated_db"
        self.table_name = "helpscout_customers"

        self.ssm = boto3.client("ssm", region_name=self.region)
        self.http_client = HTTPIngest(args['helpscout_api_base_url'], args['helpscout_app_id'], args['helpscout_app_secret'])
        self.fetcher = Fetch(self.http_client)

        self.spark = SparkSession.builder \
            .appName("HelpScoutIngestJob") \
            .config(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{self.catalog_name}.warehouse", f"s3://{self.bucket}") \
            .config(f"spark.sql.catalog.{self.catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config(f"spark.sql.catalog.{self.catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config(f"spark.sql.catalog.{self.catalog_name}.glue.lakeformation-enabled", "true") \
            .config(f"spark.sql.catalog.{self.catalog_name}.glue.skip-name-validation", "true") \
            .config("spark.sql.defaultCatalog", self.catalog_name) \
            .getOrCreate()

    def get_last_timestamp(self):
        try:
            resp = self.ssm.get_parameter(Name=self.ssm_param)
            return resp['Parameter']['Value']
        except self.ssm.exceptions.ParameterNotFound:
            logging.warning("No last timestamp found, doing full fetch")
            return None

    def update_last_timestamp(self, timestamp):
        self.ssm.put_parameter(
            Name=self.ssm_param,
            Value=timestamp,
            Type="String",
            Overwrite=True
        )

    def run(self):
        since_ts = self.get_last_timestamp()
        customers = self.fetcher.fetch_from_API(since_ts)
        if not customers:
            logging.info("No customers fetched.")
            return

        df = pd.DataFrame(customers)
        sdf = self.spark.createDataFrame(df)
        sdf = sdf.withColumn("created", F.to_timestamp("created"))
        sdf = sdf.withColumn("updated", F.to_timestamp("updated"))
        sdf = sdf.withColumn("Updated_On", F.current_timestamp())

        sdf.createOrReplaceTempView("temp_customers")
        condition = "t.customer_id = s.customer_id"
        full_table_name = f"{self.catalog_name}.{self.database_name}.{self.table_name}"

        self.spark.sql(f"""
            MERGE INTO {full_table_name} t
            USING temp_customers s
            ON {condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        latest_ts = df['updated'].max()
        self.update_last_timestamp(latest_ts)
        logging.info(f"Ingest completed. Upserted {len(df)} records. New latest timestamp: {latest_ts}")

# Main
if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, [
        'bucket_name',
        'region_name',
        'ssm_param_name',
        'helpscout_app_id',
        'helpscout_app_secret',
        'helpscout_api_base_url'
    ])
    job = RunJob(args)
    job.run()
