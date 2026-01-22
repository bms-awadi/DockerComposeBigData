import json
from kafka import KafkaConsumer

# Configuration
KAFKA_TOPIC = "weather_transformed"
KAFKA_BROKER = "kafka:9092"


def main():
    try:
        print(f"En attente de messages sur le topic '{KAFKA_TOPIC}'...")

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        for msg in consumer:
            print(f"Message reçu : {msg.value}")

    except Exception as e:
        print(f"Erreur Kafka Consumer : {e}")


if __name__ == "__main__":
    main()
