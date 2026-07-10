from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. إنشاء الـ Spark Session مع إضافة حزم أزور للكونتينر
spark = (
    SparkSession.builder
    .appName("Historical Gold Data to Processed")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6")
    .getOrCreate()
)

# إعدادات أزور (تأكدي من الـ Access Key الخاص بكِ)
STORAGE_ACCOUNT_NAME = "golddatalake001"
STORAGE_ACCOUNT_KEY = "qYhHflqebsoY1OX5djCoUMWsxUMmeJ7mtMtTgFs8H50WOkYKij5c/1GHpPhTC16ultCiZ/DLAWhW+AStElH2Kw=="

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    STORAGE_ACCOUNT_KEY
)

# 2. Extract: قراءة ملف الـ CSV المحلي
df = spark.read.csv(
    "spark/output_file (1).csv",
    header=True,
    inferSchema=True
)

# 3. Transform: تغيير اسم العمود الأول وعمل الكلينينج وتظبيط الأنواع
df_cleaned = df.withColumnRenamed("_c0", "Date")

# تظبيط نوع عمود السعر ليصبح رقم عشري صريح (لو كان فيه مشكلة)
if "price" in df_cleaned.columns:
    df_cleaned = df_cleaned.withColumn("price", col("price").cast("double"))

print("✨ Data Cleaned Locally. Starting upload to Azure 'processed'...")

# 4. Load: رفع الداتا المنظفة لحاوية الـ processed على أزور بصيغة Parquet
processed_path = f"abfss://processed@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/historical_gold_processed"

df_cleaned.write \
    .mode("overwrite") \
    .parquet(processed_path)

print("🚀 Success! Historical data is now in Azure Processed Container!")