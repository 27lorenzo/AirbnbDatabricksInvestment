# Databricks notebook source
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import FloatType, StringType
import requests
from pyspark.sql.functions import col, when, regexp_replace, expr, length


df_kamernet = spark.read.parquet("/tmp/df_kamernet.parquet")
df_airbnb = spark.read.parquet("/tmp/df_airbnb.parquet")
df_kamernet_ids = spark.read.parquet("/tmp/df_kamernet_ids.parquet")

# COMMAND ----------

# Extract from additionalCostsRaw, rent, deposit and registrationCost only the number
def extract_number_udf(column):
    return regexp_extract(column, regexp_pattern, 1).cast(FloatType())

columns_to_transform = ["deposit", "areaSqm", "matchCapacity", "registrationCost", "additionalCostsRaw", "rent"]
regexp_pattern = r"(\d+)"
for column_name in columns_to_transform:
    df_kamernet_ids = df_kamernet_ids.withColumn(column_name, extract_number_udf(df_kamernet_ids[column_name]))

# Fill NULL values with 0
columns_to_fill = ["registrationCost", "additionalCostsRaw", "rent"]

for column_name in columns_to_fill:
    df_kamernet_ids = df_kamernet_ids.na.fill(0, subset=[column_name])
    
df_kamernet_ids.show(1, truncate=False, vertical=True)

# COMMAND ----------

# Fill rows without postal code
# Check if there are rows in df_rentals or df_airbnb without postal code
print(df_kamernet.filter(col("postalCode").isNull()).count()) # Output = 0
print(df_airbnb.filter(col("zipcode").isNull()).count()) # Output = 2253

def get_postal_code(latitude, longitude):
    url = f'https://nominatim.openstreetmap.org/reverse?format=json&lat={latitude}&lon={longitude}'
    
    try:
        response = requests.get(url)
        data = response.json()

        if 'address' in data and 'postcode' in data['address']:
            postal_code = data['address']['postcode']
            return postal_code
        else:
            return None

    except Exception as e:
        print(f"Error in the request API: {e}")
        return None

# COMMAND ----------

from pyspark.sql import Window
list_group_features = ["zipcode"]
window_spec = Window.partitionBy(list_group_features)


# COMMAND ----------

get_postal_code_udf = spark.udf.register("get_postal_code", get_postal_code, StringType())

reg_expression = r'\b(\d{4}\s*[A-Z0-9]+)\b'

df_airbnb_filled = df_airbnb.withColumn(
    "zipcode",
    when((col("zipcode").isNull()) | (col("zipcode") == ""), get_postal_code_udf(col("latitude"), col("longitude")))
    .when((col("zipcode") == "b") | (col("zipcode") == "0"), get_postal_code_udf(col("latitude"), col("longitude")))
    .when((col("zipcode") == "1079 HH Amsterdam"), regexp_extract(col("zipcode"), reg_expression, 1))
    .when((col("zipcode") == "Nederland 1091 TS"), regexp_extract(col("zipcode"), reg_expression, 1))
    .when(col("zipcode").rlike("^\\d{4}$"), get_postal_code_udf(col("latitude"), col("longitude")))
    .otherwise(col("zipcode"))
)

# Unify format to 1052WL (without space)
df_airbnb_cleaned = df_airbnb_filled.withColumn(
    "zipcode",
    regexp_replace(col("zipcode"), "[^0-9a-zA-Z]+", "")
)

# Show the resulting DataFrame
df_airbnb.show()
df_airbnb_cleaned.show()

# COMMAND ----------

# Check if there are rows with more than 6 characters (1111AA)
outlier_rows = df_airbnb.filter(length(col("zipcode")) > 6)
outlier_rows.show()

# COMMAND ----------

df_kamernet_selected_cols = df_kamernet_ids.select(
    "postalCode",
    "areaSqm",
    "matchCapacity",
    "availability",
    "rent",
    "additionalCostsRaw",
    "registrationCost",
)
df_airbnb_selected_cols = df_airbnb_cleaned.select(
    "zipcode",
    "room_type",
    "accommodates",
    "bedrooms",
    "price",
    "review_scores_value"
)
df_kamernet_selected_cols.show()
df_kamernet_selected_cols.write.mode("overwrite").parquet("/tmp/df_kamernet_selected_cols.parquet")

df_airbnb_selected_cols.show()

# COMMAND ----------

# Get different room_type values
distinct_room_types = df_airbnb_selected_cols.select("room_type").distinct()
distinct_room_types.show()

# There are 172 Privates room with 0, 2, 3, 4 or 5 bedrooms -> Discard them
number_private_airbnbs = df_airbnb_selected_cols.where((col("room_type") == "Private room")&(col("bedrooms") != "1.0")).count()
print(f"Number of Private airbnbs with != 1 bedroom: {number_private_airbnbs}")
df_airbnb_filtered = df_airbnb_selected_cols.filter(~((col("room_type") == "Private room") & (col("bedrooms") != 1.0)))

number_private_airbnbs_after = df_airbnb_filtered.where((col("room_type") == "Private room")&(col("bedrooms") != "1.0")).count()
print(f"Number of Private airbnbs with != 1 bedroom: {number_private_airbnbs_after}")
df_airbnb_filtered.show()
df_airbnb_filtered.write.mode("overwrite").parquet("/tmp/df_airbnb_filtered.parquet")

# All shared rooms have only 1 bedroom
number_airbnbs_shared = df_airbnb_selected_cols.select("bedrooms").where((col("room_type") == "Shared room")&(col("bedrooms") != "1.0")).count()
print(number_airbnbs_shared)
