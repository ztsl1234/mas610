import os
import glob
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, lit, current_date
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

spark = SparkSession.builder \
    .appName("MAS610_output") \
    .getOrCreate()

# Define schemas
accounts_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
])

loans_schema = StructType([
    StructField("loan_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("loan_type", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("principal_amount", DecimalType(18, 2), True),
])

collateral_schema = StructType([
    StructField("collateral_id", StringType(), True),
    StructField("loan_id", StringType(), True),
    StructField("collateral_type", StringType(), True),
    StructField("collateral_value", DecimalType(18, 2), True),
])

# Load data from CSV files
accounts_df = spark.read.csv("accounts.csv", header=True, schema=accounts_schema)
loans_df = spark.read.csv("loans.csv", header=True, schema=loans_schema)
collateral_df = spark.read.csv("collaterals.csv", header=True, schema=collateral_schema)

# transform data

loans_with_customer = loans_df.join(accounts_df, "account_id", "left")

# Left join with collateral to include all loans including those with no collateral
final_df = loans_with_customer.join(collateral_df, "loan_id", "left")

# Calculations
mas610_loans_advances = final_df.withColumn(
    "collateral_value_clean", coalesce(col("collateral_value"), lit(0.00))
).withColumn(
    "secured_portion",
    when(col("collateral_value").isNull(), lit(0.00))
    .when(col("principal_amount") > col("collateral_value"), col("collateral_value"))
    .otherwise(col("principal_amount"))
).withColumn(
    "unsecured_portion",
    when(col("collateral_value").isNull(), col("principal_amount"))
    .when(col("principal_amount") > col("collateral_value"), col("principal_amount") - col("collateral_value"))
    .otherwise(lit(0.00))
).withColumn(
    "risk_weight", lit(100)
).withColumn(
    "reporting_date", current_date()
).select(
    col("loan_id"),
    col("customer_id"),
    col("loan_type"),
    col("currency"),
    col("principal_amount"),
    col("collateral_id"),
    col("collateral_value_clean").alias("collateral_value"),
    col("secured_portion"),
    col("unsecured_portion"),
    col("risk_weight"),
    col("reporting_date")
)

mas610_loans_advances.show()

output_path = "./mas610_output"

output_path = "./mas610_output"
final_json_filename = os.path.join(output_path, "mas610_output.json")
final_parquet_filename = os.path.join(output_path, "mas610_output.parquet")

temp_json_path = os.path.join(output_path, "temp_json")
temp_parquet_path = os.path.join(output_path, "temp_parquet")

#json
mas610_loans_advances.coalesce(1).write.mode("overwrite").json(temp_json_path)

temp_json_file = glob.glob(f"{temp_json_path}/part-*.json")[0]

os.rename(temp_json_file, final_json_filename)
print(f"Successfully created single JSON file: {final_json_filename}")

# parquet
mas610_loans_advances.coalesce(1).write.mode("overwrite").parquet(temp_parquet_path)

temp_parquet_file = glob.glob(f"{temp_parquet_path}/part-*.parquet")[0]

os.rename(temp_parquet_file, final_parquet_filename)
print(f"Successfully created single Parquet file: {final_parquet_filename}")

#clean up temp files
shutil.rmtree(os.path.dirname(temp_json_file))
shutil.rmtree(os.path.dirname(temp_parquet_file))
