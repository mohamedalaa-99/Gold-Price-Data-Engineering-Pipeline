from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==========================================
# SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")
    .appName("GoldPriceETL")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.hadoop.fs.abfss.impl",
        "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem"
    )
    .config(
        "spark.hadoop.fs.azure.account.auth.type.golddatalakee011.dfs.core.windows.net",
        "OAuth"
    )
    .getOrCreate()
)
# ==========================================
# AZURE DATA LAKE AUTHENTICATION
# ==========================================

storage_account = "golddatalakee011"
CLIENT_ID = "107d781b-fe43-4d33-a2a8-23530cfa7e38"
CLIENT_SECRET = "TI98Q~c26UV6JsNTFspgY4wfvTPDe8b-RLisFazz"
TENANT_ID = "0bc92751-071a-4e2c-a48b-633206fef374"

spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    CLIENT_ID

)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    CLIENT_SECRET
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
)

# ==========================================
# DATA LAKE PATHS
# ==========================================

RAW_CONTAINER = "raw"
SILVER_CONTAINER = "silver"

STREAM_PATH = (
    f"abfss://{RAW_CONTAINER}@{storage_account}.dfs.core.windows.net/"
    "stream_data/"
)

BATCH_PATH = (
    f"abfss://{RAW_CONTAINER}@{storage_account}.dfs.core.windows.net/"
    "historical_batch/"
)

OUTPUT_PATH = (
    f"abfss://{SILVER_CONTAINER}@{storage_account}.dfs.core.windows.net/"
    "processed/"
)

# ==========================================
# READ STREAM DATA
# ==========================================

stream_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("multiLine", "true")
    .json(STREAM_PATH + "*.json")
)
print("=" * 80)
print("Schema")
stream_df.printSchema()

print("=" * 80)
print("Columns")
print(stream_df.columns)

print("=" * 80)
stream_df.show(5, truncate=False)

if "_corrupt_record" in stream_df.columns:
    raise Exception("Spark is reading corrupted JSON files.")
# ==========================================
# CLEAN STREAM DATA
# ==========================================

stream_clean = (
    stream_df
    .withColumnRenamed("price", "gold_price")
    .withColumn(
        "event_date",
        to_date(col("updatedAt"))
    )
    .withColumn(
        "source",
        lit("gold_api")
    )
    .withColumn(
        "ingestion_type",
        lit("stream")
    )
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    .select(
        "event_date",
        "gold_price",
        "currency",
        "source",
        "ingestion_type",
        "ingestion_timestamp"
    )
)

# ==========================================
# READ BATCH DATA
# ==========================================

batch_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(BATCH_PATH)
)

# ==========================================
# CLEAN BATCH DATA
# ==========================================

batch_clean = (
    batch_df
    .withColumn(
        "event_date",
        to_date(col("event_date"))
    )
    .withColumn(
        "gold_price",
        col("gold_price").cast("double")
    )
    .withColumn(
        "currency",
        lit("USD")
    )
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    .select(
        "event_date",
        "gold_price",
        "currency",
        "source",
        "ingestion_type",
        "ingestion_timestamp"
    )
)

# ==========================================
# DATA QUALITY CHECKS
# ==========================================

stream_clean = (
    stream_clean
    .filter(col("gold_price").isNotNull())
    .filter(col("gold_price") > 0)
    .filter(col("event_date").isNotNull())
    .dropDuplicates()
)

batch_clean = (
    batch_clean
    .filter(col("gold_price").isNotNull())
    .filter(col("gold_price") > 0)
    .filter(col("event_date").isNotNull())
    .dropDuplicates()
)

# ==========================================
# MERGE STREAM + BATCH
# ==========================================

gold_df = stream_clean.unionByName(batch_clean)

# ==========================================
# DERIVED ANALYTICS COLUMNS
# ==========================================

gold_df = (
    gold_df
    .withColumn("year", year("event_date"))
    .withColumn("month", month("event_date"))
    .withColumn("quarter", quarter("event_date"))
)

# ==========================================
# WRITE SILVER LAYER
# ==========================================

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("year")
    .save(OUTPUT_PATH)
)

print("Gold data successfully written to Silver layer.")

try:
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year")
        .save(OUTPUT_PATH)
    )

    print("Done")

except Exception as e:
    import traceback
    traceback.print_exc()
    raise