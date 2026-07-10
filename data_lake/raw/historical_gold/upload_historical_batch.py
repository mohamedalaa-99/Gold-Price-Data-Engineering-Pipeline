from azure.storage.blob import BlobServiceClient
import os

# 1. إعدادات الاتصال بـ Azure
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=golddatalake001;AccountKey=qYhHflqebsoY1OX5djCoUMWsxUMmeJ7mtMtTgFs8H50WOkYKij5c/1GHpPhTC16ultCiZ/DLAWhW+AStElH2Kw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "raw"

# 2. تحديد مسار الملف ديناميكياً (ليقرأ من نفس مجلد السكريبت الحالي)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_FILE_PATH = os.path.join(BASE_DIR, "historical_gold.csv")

# 3. تحديد اسم المجلد والملف داخل أزور
AZURE_BLOB_NAME = "historical_batch/historical_gold.csv"

def upload_batch_to_datalake():
    try:
        # التأكد أولاً من وجود الملف في المسار الديناميكي قبل البدء
        if not os.path.exists(LOCAL_FILE_PATH):
            raise FileNotFoundError(f"Could not find file at estimated path: {LOCAL_FILE_PATH}")

        # إنشاء نقطة الاتصال والرفع
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=AZURE_BLOB_NAME)

        print(f"⏳ Uploading {LOCAL_FILE_PATH} to Azure Data Lake...")
        
        with open(LOCAL_FILE_PATH, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            
        print("✅ Batch File Uploaded Successfully to 'raw' container!")
        
    except Exception as e:
        print(f"❌ Error occurred: {e}")

if __name__ == "__main__":
    upload_batch_to_datalake()