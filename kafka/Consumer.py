import datetime
import json
import sys

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from kafka import KafkaConsumer

# ==========================================================
# AZURE DATA LAKE
# ==========================================================

storage_account = "golddatalakee011"

CLIENT_ID = "107d781b-fe43-4d33-a2a8-23530cfa7e38"
CLIENT_SECRET = "TI98Q~c26UV6JsNTFspgY4wfvTPDe8b-RLisFazz"
TENANT_ID = "0bc92751-071a-4e2c-a48b-633206fef374"

credential = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
)

service_client = DataLakeServiceClient(
    account_url=f"https://{storage_account}.dfs.core.windows.net",
    credential=credential,
)

filesystem = service_client.get_file_system_client("raw")

print("Connected To Azure Data Lake")

# ==========================================================
# KAFKA
# ==========================================================

consumer = KafkaConsumer(
    "gold-price-topic",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="gold-group",
    consumer_timeout_ms=60000,
)

print("Waiting For Kafka Message...")

try:

    for message in consumer:

        data = message.value

        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        file_name = f"stream_data/gold_price_{timestamp}.json"

        file_client = filesystem.get_file_client(file_name)

        content = json.dumps(data, indent=4)

        file_client.create_file()

        file_client.append_data(
            data=content,
            offset=0,
            length=len(content)
        )

        file_client.flush_data(len(content))

        print(f"Uploaded -> {file_name}")

        break

    consumer.close()

    print("Consumer Finished Successfully")

except Exception as e:

    print(e)

    consumer.close()

    sys.exit(1)