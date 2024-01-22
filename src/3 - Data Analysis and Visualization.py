# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
import matplotlib.pyplot as plt


df_kamernet_selected_cols = spark.read.parquet("/tmp/df_kamernet_selected_cols.parquet")
df_airbnb_filtered = spark.read.parquet("/tmp/df_airbnb_filtered.parquet")

columns_to_check = ["registrationCost", "additionalCostsRaw", "rent"]
for column_name in columns_to_check:
    df_nulls = df_kamernet_selected_cols.filter(col(column_name).isNull())
    df_nulls.show()


# COMMAND ----------

# Calculate total income
df_kamernet_income = df_kamernet_selected_cols.withColumn(
    "income", col("rent") + col("additionalCostsRaw") + col("registrationCost"))
# There Entire home/apt airbnbs with 0 bedrooms -> income = price
df_airbnb_income = df_airbnb_filtered.withColumn(
    "income",
    when((col("room_type") == "Entire home/apt") & ((col("bedrooms") == 0.0) | col("bedrooms").isNull()), col("price"))
    .otherwise(col("price") * col("bedrooms"))
)
df_kamernet_income.show()
df_airbnb_income.show()


# COMMAND ----------

# Group by postal code and save it into data/output
output_path = "file:/Workspace/Repos/lorenzo.borreguerocorton@cgi.com/AirbnbDatabricksInvestment/data/output/"
path_kamernet = output_path + "/df_kamernet_grouped.parquet"

df_kamernet_grouped = df_kamernet_income.groupBy("postalCode").agg(F.mean("income").alias("avg_kamernet_income"))
df_kamernet_grouped.write.mode("overwrite").parquet(path_kamernet)
df_kamernet_grouped.show()

# COMMAND ----------

path_airbnb = output_path + "/df_airbnb_grouped.parquet"

df_airbnb_grouped = df_airbnb_income.groupBy("zipcode").agg(F.mean("income").alias("avg_airbnb_income"))
df_airbnb_grouped.write.mode("overwrite").parquet(path_airbnb)
df_airbnb_grouped.show()

# COMMAND ----------

# Compare kamernet and airbnb income in a joined table
path_df_final = output_path + "/df_final.parquet"
df_final = df_kamernet_grouped.join(df_airbnb_grouped, df_kamernet_grouped.postalCode == df_airbnb_grouped.zipcode, how="inner") \
    .select("postalCode", "avg_kamernet_income", "avg_airbnb_income") \
    .orderBy("avg_airbnb_income", ascending=False)

df_final.write.mode("overwrite").parquet(path_df_final)

df_final.show()

# COMMAND ----------

# Sort dataframe by avg_kamernet_income
df_kamernet_sorted = df_final.orderBy("avg_kamernet_income", ascending=False).limit(20).toPandas()

fig, axs = plt.subplots(1, 2, figsize=(15, 6))

bars1 = axs[0].barh(df_kamernet_sorted['postalCode'], df_kamernet_sorted['avg_kamernet_income'], label='Avg Kamernet Income')
bars2 = axs[0].barh(df_kamernet_sorted['postalCode'], df_kamernet_sorted['avg_airbnb_income'], label='Avg Airbnb Income', alpha=0.7)

axs[0].set_xlabel('Income')
axs[0].set_title('Top 20 Postal Codes by Avg Kamernet Income')
axs[0].legend()

df_airbnb_sorted = df_final.orderBy("avg_airbnb_income", ascending=False).limit(20).toPandas()

bars3 = axs[1].barh(df_airbnb_sorted['postalCode'], df_airbnb_sorted['avg_kamernet_income'], label='Avg Kamernet Income')
bars4 = axs[1].barh(df_airbnb_sorted['postalCode'], df_airbnb_sorted['avg_airbnb_income'], label='Avg Airbnb Income', alpha=0.7)

axs[1].set_xlabel('Income')
axs[1].set_title('Top 20 Postal Codes by Avg Airbnb Income')
axs[1].legend()

plt.tight_layout()
plt.show()

