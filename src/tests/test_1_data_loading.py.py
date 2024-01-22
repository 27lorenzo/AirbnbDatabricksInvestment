# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import os
import unittest


s3_bucket = "s3://airbnb-assessment-data"
s3_airbnb_path = "/airbnb.csv"
s3_rentals_path = "/rentals.json"

spark = SparkSession.builder.appName("UnitTest").getOrCreate()

class TestDataLoading(unittest.TestCase):
    
    def test_data_loading(self):
        # Test loading data into Spark DataFrames
        # Load data into a DataFrame
        df_kamernet = spark.read.options(header='true', inferSchema='true').json(s3_bucket + s3_rentals_path)
        df_airbnb = spark.read.options(header='true').csv(s3_bucket + s3_airbnb_path)
        self.assertNotEqual(df_kamernet.count(), 0)
        self.assertNotEqual(df_airbnb.count(), 0)
    
    def test_id_conversion(self):
        # Check all values in _id field of kamernet dataset are string type
        df_kamernet_ids = spark.read.parquet("/tmp/df_kamernet_ids.parquet")
        id_data_types = df_kamernet_ids.select("_id").dtypes

        for field, data_type in id_data_types:
            self.assertEqual(data_type, "string", f"Field '{field}' should be StringType")

test_runner = unittest.main(argv=[''], verbosity=2, exit=False)

if not test_runner.result.wasSuccessful():
    raise Exception(
        f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed; see logs above")
