# Databricks notebook source
# MAGIC %md # Data Pipeline for Real Estate Investment
# MAGIC
# MAGIC This data pipeline aims to provide a classification of the most profitable postal codes for real estate investment.
# MAGIC
# MAGIC ## Data 
# MAGIC The pipeline involves the analysis of two datasets:
# MAGIC - airbnb.csv: csv file containing 9913 airbnb entire homes, private rooms and shared rooms.  
# MAGIC - rentals.json: json file with 46723 rooms posted in Kamernet.nl
# MAGIC
# MAGIC Both datasets are securely stored in an AWS S3 bucket to mitigate data loss risks.
# MAGIC
# MAGIC ## Pipeline Overview
# MAGIC The pipeline consists of three consecutive notebooks designed for data loading, analysis, and visualization.
# MAGIC
# MAGIC #### 1- Data Loading
# MAGIC
# MAGIC The first phase involves reading the two datasets from the AWS S3 Bucket. A Spark session is created with optimized configurations to improve performance and reduce running time. Across the pipeline, the datasets are stored as Parquet files in the tmp/ folder to facilitate efficient data retrieval, eliminating the necessity to rerun the entire pipeline for data availability
# MAGIC
# MAGIC I opted to store the 'rentals.json' file in my local MongoDB storage because MongoDB offers a user-friendly interface for visualizing JSON data, simplifying the process of detecting inconsistencies or errors within the dataset. As MongoDB requires a string id field, I converted the _id field of 'rentals.json' from an array type to a string type for compatibility.
# MAGIC
# MAGIC All the credentials (AWS and MongoDB) are stored in the Databricks cluster environmental variables. The use of Databricks Secret management in this task was considered, but because time restrictions, I discarded. 
# MAGIC
# MAGIC #### 2- Data Analysis
# MAGIC The data analysis begins by removing all non-numeric characters from the money-related fields (additionalCostsRaw, rent, deposit, registrationCost), to simplify the future operations and calculations. Additionally, I observed that certain rows contain null values in one or more of these columns, and to address this, I filled those missing values with zeros.
# MAGIC
# MAGIC ###### API Call
# MAGIC In the 'airbnb.csv' dataset, 2254 rows lack a postal code, constituting over 22% of the total data. Moreover, many postal codes are incomplete, lacking the two letters. These factors led me to improve the postal code data in 'airbnb.csv' by making an API call.
# MAGIC
# MAGIC The API provided in the assessment was helpful, but it only provided the four numbers of the postal code, not the two letters. Consequently, I did further research and discovered the openstreetmap.org API, with which the complete postal code (4 numbers + 2 letters) can be retrieved when given latitude and longitude coordinates as input parameters. After some data analysis, I identified some inconsistencies within the data, based on which I made the below conditions.
# MAGIC
# MAGIC The API is called in either of the following cases:
# MAGIC - zipcode field is null
# MAGIC - zipcode field is empty
# MAGIC - zipcode field only contains 4 numbers without letters
# MAGIC - zipcode field is one of the outliers "b" or "0"
# MAGIC
# MAGIC For the rows with zipcode as "1079 HH Amsterdam", "1016 BL Amsterdam", "1056 BL Amsterdam" "Nederland 1091 TS", a regular expression is used to extract only the postal code. 
# MAGIC
# MAGIC To avoid future complications, I unified the postal code format by removing the space between numbers and letters (1111AA).
# MAGIC
# MAGIC ###### Data Filtering
# MAGIC Data filtering is performed to extract subsets with the most descriptive variables in each dataset.
# MAGIC
# MAGIC In the Airbnb dataset, I encountered some ambiguity regarding the difference between Private room and Shared room in terms of revenue. To address this, I analyzed both categories and identified 172 Private rooms with 0, 2, 3, 4, or 5 bedrooms, which, in my opinion, seems inconsistent. This discrepancy may have arisen from potential errors in the information provided by the Airbnb hosts. Consequently, I decided to exclude these entries from the analysis. On the other hand, all Shared rooms in the dataset were found to have only 1 bedroom.
# MAGIC
# MAGIC #### 3- Data Analysis and Visualization:
# MAGIC
# MAGIC ###### Income metric calculation
# MAGIC In this third and last phase of the pipeline, I calculate the total income per row. This is calculate differently based on the fields of each dataset. 
# MAGIC - Kamernet (rentals.json): the total income is calculated by summing the rent, additionalCostsRaw, and registrationCost. I excluded deposit as I don't consider it as a revenue source. I included registrationCost, although its meaning is not clear to me.
# MAGIC - Airbnb (airbnb.csv): for airbnbs with > 1 bedrooms, the income is determined by multiplying the price by the number of bedrooms. For airbnbs with < 1 bedrooms, the price serves as the income. 
# MAGIC
# MAGIC * I assumed that Airbnb listings with 0 bedrooms were errors made by the owner, so I treated all of them as 1-bedroom Airbnbs.
# MAGIC
# MAGIC ###### Data grouping and visualization
# MAGIC Thanks to the fact that the postal codes are in an unified format, I was able to group them and calculate the average income per postal code for both datasets. I created a join table on postal code to compare the incomes per postal code and dataset. The final dataset is saved in the repo folder data/output.
# MAGIC
# MAGIC Finally, I visualized the final results with two plots:
# MAGIC - On the left, top 20 postal codes with the highest average revenue in Kamernet (blue colour)
# MAGIC - On the left, top 20 postal codes with the highest average revenue in Airbnb (orange colour)

# COMMAND ----------


