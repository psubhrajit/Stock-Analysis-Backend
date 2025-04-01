from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers
import json

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stock_data"

ES_HOST = "http://localhost:9200"
INDEX_NAME = "stock-data"

es = Elasticsearch([ES_HOST])

if not es.ping():
    raise ValueError("Elasticsearch connection failed")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"Consuming messages from Kafka topic: {TOPIC_NAME}")

try:
    for message in consumer:
        stock_data = message.value
        print(f"Consumed: {stock_data}")

        # Index the stock data in Elasticsearch
        res = es.index(index=INDEX_NAME, body=stock_data)
        print(f"Document indexed: {res['_id']}")
except Exception as e:
    print(f"Error consuming messages from Kafka: {e}")
except KeyboardInterrupt:
    print("Stopping Kafka consumer")
finally:
    consumer.close()
