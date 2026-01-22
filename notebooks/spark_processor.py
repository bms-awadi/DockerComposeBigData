from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# Initialisation de la session Spark liée au Master Docker
spark = SparkSession.builder \
    .appName("WeatherStreamingProcess") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Schéma des données météo
schema = StructType([
    StructField("time", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("high_wind_alert", BooleanType(), True),
    StructField("temp_f", DoubleType(), True)
])

# Lecture du flux Kafka (Topic: weather_transformed)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_transformed") \
    .load()

# Conversion du JSON
weather_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Écriture vers HDFS
query = weather_df.writeStream \
    .format("json") \
    .option("path", "hdfs://namenode:9000/user/jovyan/weather_data") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .start()

print("Traitement Spark en cours... Les données arrivent sur HDFS.")
query.awaitTermination()