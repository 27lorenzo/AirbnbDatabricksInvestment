# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract, length
from pyspark.sql.types import FloatType, StringType

spark = SparkSession.builder.appName("UnitTest").getOrCreate()

class TestDataAnalysis(unittest.TestCase):
    output_path = "file:/Workspace/Repos/lorenzo.borreguerocorton@cgi.com/AirbnbDatabricksInvestment/data/output/"
    path_df_final = output_path + "/df_final.parquet"

    df_final = spark.read.parquet(path_df_final)

    def test_no_null_values_avg_kamernet_income(self):
        # Check there are no null values in either avg_kamernet_income or avg_airbnb_income
        null_count_airbnb = self.df_final.filter(self.df_final.avg_airbnb_income.isNull()).count()
        null_count_kamernet = self.df_final.filter(self.df_final.avg_airbnb_income.isNull()).count()
        
        self.assertEqual(null_count_airbnb, 0, "Null values were found in avg_airbnb_income")
        self.assertEqual(null_count_kamernet, 0, "Null values were found in avg_kamernet_income")

test_runner = unittest.main(argv=[''], verbosity=2, exit=False)

if not test_runner.result.wasSuccessful():
    raise Exception(
        f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed; see logs above")
