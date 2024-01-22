# Databricks notebook source
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
mongodb_user = os.environ.get("mongodb_user")
mongodb_pwd = os.environ.get("mongodb_pwd")

s3_bucket = "s3://airbnb-assessment-data"
s3_airbnb_path = "/airbnb.csv"
s3_rentals_path = "/rentals.json"

spark = SparkSession.builder \
    .appName("SparkApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()

# COMMAND ----------

# Ingest rentals.json in streaming from Kafka

kafka_bootstrap_servers = "kafka_bootstrap_server"
kafka_topic = "streaming_rentals"

json_schema = spark.read.json(s3_bucket + s3_rentals_path).schema

# Create a streaming DataFrame
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Write the streaming data to Parquet or any other sink
output_path = "/tmp/streaming_output"
checkpoint_location = "/tmp/checkpoint_location"

df_kamernet_streaming = df_kafka \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()
