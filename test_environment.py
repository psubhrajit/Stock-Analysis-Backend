from kafka import KafkaProducer
from elasticsearch import Elasticsearch

try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    print("Kafka producer connected successfully")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")

try:
    es = Elasticsearch(["http://localhost:9200"])
    if es.ping():
        print("Elasticsearch connected successfully")
    else:
        print("Error connecting to Elasticsearch")
except Exception as e:
    print(f"Error connecting to Elasticsearch: {e}")
