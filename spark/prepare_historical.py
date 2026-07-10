from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("Prepare Historical Data")
    .getOrCreate()
)

# Read Historical CSV
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("spark/output_file (1).csv")
)

# Fix first column name
df = df.withColumnRenamed("_c0", "Date")

# Prepare schema for future union with Kafka
historical_df = (
    df.select("Date", "USD")
      .withColumnRenamed("Date", "event_date")
      .withColumnRenamed("USD", "gold_price")
      .withColumn("source", lit("historical_csv"))
      .withColumn("ingestion_type", lit("batch"))
)

print("===== HISTORICAL SCHEMA =====")
historical_df.printSchema()

print("===== SAMPLE DATA =====")
historical_df.show(10, truncate=False)

# Create Data Lake folder
os.makedirs(
    "data_lake/raw",
    exist_ok=True
)

# Save as CSV using Pandas
historical_df.toPandas().to_csv(
    "data_lake/raw/historical_gold.csv",
    index=False
)

print("===================================")
print("Historical file saved successfully")
print("Location: data_lake/raw/historical_gold.csv")
print("===================================")

spark.stop()