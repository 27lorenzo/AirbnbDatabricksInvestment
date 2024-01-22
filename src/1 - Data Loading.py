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
s3_postcode_geojson = "/geo/post_codes.geojson"
s3_ams_geojson = "/geo/amsterdam_areas.geojson"

spark = SparkSession.builder \
    .appName("SparkApp") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()

# COMMAND ----------

df_kamernet = spark.read.options(header='true', inferSchema='true').json(s3_bucket + s3_rentals_path)
df_kamernet.write.mode("overwrite").parquet("/tmp/df_kamernet.parquet")

df_airbnb = spark.read.options(header='true').csv(s3_bucket + s3_airbnb_path)
df_airbnb.write.mode("overwrite").parquet("/tmp/df_airbnb.parquet")

df_postcode_geojson = spark.read.json(s3_bucket + s3_postcode_geojson)
df_ams_geojson = spark.read.json(s3_bucket + s3_ams_geojson)

# COMMAND ----------

# Change _id type from array to string so that it will be readable by mongodb
def array_to_string(id_array):
    return id_array[0] if id_array else None
array_to_string_udf = udf(array_to_string, StringType())
df_kamernet_ids = df_kamernet.withColumn("_id", array_to_string_udf(df_kamernet["_id"]))
df_kamernet_ids.write.mode("overwrite").parquet("/tmp/df_kamernet_ids.parquet")

# Write json data in MongoDB
database = 'projects'
collection = 'RevoData-assessment-rent-airbnb'
connection_string = f"mongodb+srv://{mongodb_user}:{mongodb_pwd}@cluster0.ouy300o.mongodb.net/"

"""
df_kamernet_ids.write.format("mongo") \
    .option("spark.mongodb.output.uri", connection_string) \
    .option("spark.mongodb.output.database", database) \
    .option("collection",collection) \
    .mode("append").save()
"""
