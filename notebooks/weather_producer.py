import requests, json, time, os
from kafka import KafkaProducer

API_URL = "https://api.open-meteo.com/v1/forecast"
LAT, LON = 52.52, 13.41
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_transformed")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

def fetch_weather():
    params = {"latitude": LAT, "longitude": LON, "current_weather": "true"}
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("current_weather", {})

def transform_weather(record: dict) -> dict:
    if "temperature" in record:
        record["temp_f"] = record["temperature"] * 9/5 + 32
    record["high_wind_alert"] = record.get("windspeed", 0) > 10
    return record

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    print("Producer started...")
    while True:
        try:
            weather = fetch_weather()
            if weather:
                transformed = transform_weather(weather)
                producer.send(KAFKA_TOPIC, transformed)
                producer.flush()
                print("Sent:", transformed)
        except Exception as e:
            print("Error:", e)
        time.sleep(30)

if __name__ == "__main__":
    main()