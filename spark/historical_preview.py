from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Historical Preview")
    .getOrCreate()
)

# Read CSV
df = spark.read.csv(
    "spark/output_file (1).csv",
    header=True,
    inferSchema=True
)

# Rename first column
df = df.withColumnRenamed("_c0", "Date")

print("\n===== ROWS =====")
print(df.count())

print("\n===== COLUMNS =====")
print(df.columns)

print("\n===== SCHEMA =====")
df.printSchema()

print("\n===== SAMPLE DATA =====")
df.show(10, truncate=False)