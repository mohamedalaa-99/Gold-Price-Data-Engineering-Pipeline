from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = (
    SparkSession.builder
    .appName("Gold Exploration")
    .getOrCreate()
)

df = spark.read.csv(
    "spark/output_file (1).csv",
    header=True,
    inferSchema=True
)

df = df.withColumnRenamed("_c0", "Date")

print("Rows:", df.count())
print("Columns:", len(df.columns))

print("\nMissing Values:")

df.select([
    count(
        when(col(c).isNull(), c)
    ).alias(c)
    for c in df.columns
]).show()