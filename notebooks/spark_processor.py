import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count
from pyspark.sql.types import *

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_transformed")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

def main():
    spark = SparkSession.builder \
        .appName("WeatherAggregation") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    schema = StructType([
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("temp_f", DoubleType(), True),
        StructField("high_wind_alert", BooleanType(), True),
        StructField("time", StringType(), True)
    ])
    
    raw_df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    
    parsed = raw_df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", col("time").cast(TimestampType()))
    
    # Agrégation sur fenêtre temporelle de 1 minute
    agg = parsed.groupBy(window(col("event_time"), "1 minute")) \
        .agg(
            avg("temperature").alias("avg_temp_c"),
            count(col("high_wind_alert")).alias("alert_count")
        )
    
    print("Agrégation météo sur fenêtre de 1 minute")
    agg.show(truncate=False)
    print(f"Nombre de fenêtres temporelles: {agg.count()}")
    
    spark.stop()

if __name__ == "__main__":
    main()