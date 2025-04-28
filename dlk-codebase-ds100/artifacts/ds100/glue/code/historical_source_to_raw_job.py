from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as fn
from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructType, StructField, TimestampType

import boto3
from botocore.exceptions import ClientError
from datetime import timedelta, datetime as dt
from io import StringIO
import json
import pandas as pd
import requests
import sys
from time import sleep


class CamDBETL:
    def __init__(self, region_name: str, secret_name: str, glueContext, logger):
        self.region_name = region_name
        self.secret_name = secret_name
        self.dt_partition = dt.now().strftime("%Y-%m-%d")
        self.yesterday_date = dt.now() - timedelta(days=1)
        self.yesterday = self.yesterday_date.strftime("%Y-%m-%d")
        self.shipdate_from = shipdate_from
        self.shipdate_to = shipdate_to
        self.glueContext = glueContext
        self.logger = logger

    def get_secret(self) -> dict:
        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager", region_name=self.region_name
        )
        try:
            get_secret_value_response = client.get_secret_value(SecretId=self.secret_name)
        except ClientError as e:
            raise e

        secret = get_secret_value_response["SecretString"]
        return json.loads(secret)

    def api_call(
        self, base_url: str, endpoint: str, params: dict, username: str, password: str
    ) -> str:
        response = requests.get(
            base_url + endpoint, params=params, auth=(username, password), verify=False
        )
        if response.status_code == 200:
            self.logger.info(f"API CALL {response.status_code}")
            return response.text
        else:
            self.logger.error(f"API Error: {response.status_code}")
            return ""

    def extract_transform(self, config: dict) -> SparkDataFrame:
        df = pd.DataFrame()
        creds = self.get_secret()
        base_url = creds[config["base_url_key"]]
        username = creds[config["username_key"]]
        password = creds[config["password_key"]]
        center_ids = json.loads(creds["center_id"])
        endpoint = "v3/"

        # Timestamp columns for type casting
        timestamp_columns = config.get("timestamp_columns", [])

        def infer_and_cast_types(df: pd.DataFrame) -> pd.DataFrame:
            for col in df.columns:
                df[col] = df[col].astype(str)  # Ensure all columns are strings
            return df

        for center_id in center_ids:
            try:
                if config["name"] == "closedRestorationOrderExporter":
                    params = {
                        "center_id": center_id,
                        "format": "csv",
                        "shipdate_from": self.shipdate_from,
                        "shipdate_to":  self.shipdate_to,
                    }
                else:
                    params = {"center_id": center_id, "format": "csv"}

                self.logger.info(f"Calling API for center_id: {center_id}")
                data = StringIO(
                    self.api_call(base_url, endpoint, params, username, password)
                )
                tmp = pd.read_csv(data, dtype=str)
                if df.empty:
                    df = tmp
                else:
                    df = pd.concat([df, tmp])
                sleep(2)  # Rate-limiting delay
            except pd.errors.EmptyDataError:
                self.logger.info(f"No data for center_id: {center_id}")
            except Exception as e:
                self.logger.error(f"Error processing center_id {center_id}: {str(e)}")
                continue

        if df.empty:
            self.logger.info(f"No data extracted for table {config['name']}")
            return self.glueContext.spark_session.createDataFrame([], schema=None)

        # Treat all columns as strings initially
        df = infer_and_cast_types(df)

        # Define all columns as StringType
        schema = StructType(
            [StructField(col, StringType(), True) for col in df.columns]
        )

        # Convert to Spark DataFrame
        spark_df = self.glueContext.spark_session.createDataFrame(df, schema=schema)

        # Preprocess timestamp columns
        for column in timestamp_columns:
            spark_df = spark_df.withColumn(
                column,
                fn.when(
                    fn.regexp_extract(fn.col(column), r'^\d{4}-\d{2}-\d{2}.*$', 0) != "",
                    fn.to_timestamp(fn.col(column), "yyyy-MM-dd HH:mm:ss")
                ).otherwise(None)  # Replace invalid values with null
            )
            
        dt_partition = self.dt_partition  # Use the partition date defined in the class
        spark_df = spark_df.withColumn("DT_PARTITION", fn.lit(dt_partition))

        # Change columns to TimestampType
        for column in timestamp_columns:
            spark_df = spark_df.withColumn(column, spark_df[column].cast(TimestampType()))

        return spark_df

    def load_df(self, df: SparkDataFrame, load_path: str) -> None:
        if df.rdd.isEmpty():
            self.logger.info("DataFrame is empty. Skipping load.")
            return

        dynamic_frame = DynamicFrame.fromDF(df, self.glueContext, "dynamic_frame")
        sink = self.glueContext.getSink(
            path=load_path,
            connection_type="s3",
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["DT_PARTITION"],
            enableUpdateCatalog=True,
        )
        sink.setFormat("glueparquet")
        sink.writeFrame(dynamic_frame)

    def process_table(self, config: dict) -> None:
        self.logger.info(f"Processing table: {config['name']}")
        try:
            df = self.extract_transform(config)
            self.load_df(df, config["load_path"])
            self.logger.info(f"Successfully processed table: {config['name']}")
        except Exception as e:
            self.logger.error(f"Failed to process table {config['name']}: {str(e)}")


if __name__ == "__main__":
    sc = SparkContext()
    glueContext = GlueContext(sc)
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "region_name", "secret_name", "env", "shipdate_from", "shipdate_to"]
    )
    region_name = args["region_name"]
    secret_name = args["secret_name"]
    shipdate_from = args["shipdate_from"]
    shipdate_to = args["shipdate_to"]
    env = args["env"]
    logger = glueContext.get_logger()
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # Define configurations for each table
    table_configs = [
        {
            "name": "closedRestorationOrderExporter",
            "base_url_key": "base_url_closed",
            "username_key": "username_closed",
            "password_key": "password_closed",
            "load_path": f"s3://stg-dlk-{env}-ds-25-raw/CAMDBAPI/closedRestorationOrderExporter/",
            "timestamp_columns": [
                "order_date", "order_date_plant_timezone", "slot_closed_date",
                "slot_closed_date_plant_timezone", "timestamp_sales_order",
                "timestamp_production_order", "arrival_date"
            ],
        },
    ]

    etl = CamDBETL(region_name=region_name, secret_name=secret_name, glueContext=glueContext, logger=logger)

    # Process each table
    for config in table_configs:
        etl.process_table(config)

    job.commit()
