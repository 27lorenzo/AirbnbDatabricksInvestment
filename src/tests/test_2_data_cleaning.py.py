# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract, length
from pyspark.sql.types import FloatType, StringType

spark = SparkSession.builder.appName("UnitTest").getOrCreate()

class TestDataCleaning(unittest.TestCase):
    df_kamernet_cleaned = spark.read.parquet("/tmp/df_kamernet_cleaned.parquet")
    df_airbnb_filtered = spark.read.parquet("/tmp/df_airbnb_filtered.parquet")

    def test_numeric_values_in_kamernet(self):
        # Check that df_kamernet_cleaned does not contain non-numeric characters in ["deposit", "areaSqm", "matchCapacity", "registrationCost", "additionalCostsRaw", "rent"]
        regexp_pattern = r"(\d+)"
        columns_to_check = ["deposit", "areaSqm", "matchCapacity", "registrationCost", "additionalCostsRaw", "rent"]
        for column_name in columns_to_check:
            result = self.df_kamernet_cleaned.filter(~col(column_name).rlike(regexp_pattern)).count()
            self.assertEqual(result, 0)

    def test_no_null_values_in_kamernet(self):
        # Check that df_kamernet_cleaned does not contain null values in ["registrationCost", "additionalCostsRaw", "rent"]
        columns_to_check = ["registrationCost", "additionalCostsRaw", "rent"]
        for column_name in columns_to_check:
            result = self.df_kamernet_cleaned.filter(col(column_name).isNull()).count()
            self.assertEqual(result, 0)

    def test_format_zipcode_in_airbnb(self):
        # Check that df_airbnb_filtered has unified format in zipcode column (1111AA)
        result = self.df_airbnb_filtered.filter(length(col("zipcode")) > 6).count()
        self.assertEqual(result, 0)

    def test_private_rooms_in_airbnb(self):
        # Check that in df_airbnb_filtered, there are no Private rooms with number of bedrooms != 1
        result = self.df_airbnb_filtered.where((col("room_type") == "Private room") & (col("bedrooms") != "1.0")).count()
        self.assertEqual(result, 0)

test_runner = unittest.main(argv=[''], verbosity=2, exit=False)

if not test_runner.result.wasSuccessful():
    raise Exception(
        f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed; see logs above")
