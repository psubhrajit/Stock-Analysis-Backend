import random
import time
from kafka import KafkaProducer
import json 

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stock_data"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # serialize data as JSON
)

def generate_stock_data():
    symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "META", "TSLA"]
    return {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "symbol": random.choice(symbols),
        "price": round(random.uniform(100, 500), 2),
        "volume": random.randint(1000, 5000)
    }

try:
    print(f"Sending stock data to Kafka topic: {TOPIC_NAME}")
    while True:
        stock_data = generate_stock_data()
        producer.send(TOPIC_NAME, value=stock_data)
        print(f"Produced: {stock_data}")
        time.sleep(1)
except Exception as e:
    print(f"Error sending stock data to Kafka: {e}")
except KeyboardInterrupt:
    print("Stopping Kafka producer")
finally:
    producer.close()