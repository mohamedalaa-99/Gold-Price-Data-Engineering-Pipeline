from kafka import KafkaProducer
import requests
import json
import sys

TOPIC_NAME = "gold-price-topic"
API_URL = "https://api.gold-api.com/price/XAU"

print("========== GOLD PRODUCER ==========")

try:

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Connected to Kafka")

    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    api_data = response.json()

    producer.send(TOPIC_NAME, api_data)
    producer.flush()

    producer.close()

    print("Message sent Successfully")

except Exception as e:
    print(e)
    sys.exit(1)