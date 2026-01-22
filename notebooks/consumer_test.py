import json, os
from kafka import KafkaConsumer

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_transformed")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    print(f"Listening on {KAFKA_TOPIC}...")
    for msg in consumer:
        print(f"Received: {msg.value}")

if __name__ == "__main__":
    main()