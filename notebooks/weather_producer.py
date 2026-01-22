import requests
import json
import time
from kafka import KafkaProducer

# Configuration - On utilise le nom du service docker 'kafka'
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "weather_transformed"
API_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather():
    params = {"latitude": 48.8566, "longitude": 2.3522, "current_weather": "true"}
    try:
        r = requests.get(API_URL, params=params)
        return r.json().get("current_weather", {})
    except:
        return None


def transform_weather(data):
    # Transformation simple : ajout d'un flag vent fort
    data["high_wind_alert"] = data.get("windspeed", 0) > 20
    data["temp_f"] = round(data.get("temperature", 0) * 9 / 5 + 32, 2)
    return data


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Streaming démarré sur {KAFKA_TOPIC}...")
while True:
    raw_data = fetch_weather()
    if raw_data:
        message = transform_weather(raw_data)
        producer.send(KAFKA_TOPIC, message)
        print(f"Envoyé: {message}")
    time.sleep(10)  # On envoie toutes les 10 secondes pour le test
