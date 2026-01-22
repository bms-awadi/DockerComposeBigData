from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json, col, window, avg, count 
from pyspark.sql.types import StructType, StructField, DoubleType, BooleanType, StringType, TimestampType 
from hdfs import InsecureClient
from io import StringIO


def main(): 
    spark = SparkSession.builder \
        .appName("WeatherAggregation") \
        .master("spark://spark-master:7077") \
        .config(
            "spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"
        ) \
        .getOrCreate() 
    
    spark.sparkContext.setLogLevel("WARN") 
    
    schema = StructType([
        StructField("temperature", DoubleType(), True), 
        StructField("windspeed", DoubleType(), True), 
        StructField("temp_f", DoubleType(), True), 
        StructField("high_wind_alert", BooleanType(), True), 
        StructField("time", StringType(), True) 
    ]) 
    
    raw_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "weather_transformed") \
        .option("startingOffsets", "earliest") \
        .load() 
    
    json_df = raw_df.selectExpr("CAST(value AS STRING) as json") 
    
    parsed = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*") 
    
    parsed = parsed.withColumn("event_time", col("time").cast(TimestampType())) 
    
    agg = parsed.groupBy(
        window(col("event_time"), "1 minute")
    ).agg(
        avg("temperature").alias("avg_temp_c"), 
        count(col("high_wind_alert")).alias("alert_count")
    ) 
    
    agg.show(truncate=False) 
    
    # Sauvegarde dans HDFS avec InsecureClient
    hdfs_url = "http://namenode:9870"
    hdfs_dir = "/user/jovyan/weather"
    hdfs_path = "/user/jovyan/weather/aggregated_data.csv"
    
    # Créer le client HDFS
    client = InsecureClient(hdfs_url, user='jovyan')
    
    # Créer le dossier dans HDFS
    try:
        client.makedirs(hdfs_dir)
        print(f"Dossier cree dans HDFS: {hdfs_dir}")
    except Exception as e:
        print(f"Le dossier existe deja ou erreur: {e}")
    
    # Convertir le DataFrame Spark en Pandas
    pandas_df = agg.toPandas()
    
    # Convertir en CSV
    csv_buffer = StringIO()
    pandas_df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    # Ecrire dans HDFS
    with client.write(hdfs_path, overwrite=True, encoding='utf-8') as writer:
        writer.write(csv_data)
    
    print(f"Donnees sauvegardees dans HDFS: {hdfs_path}")
    print(f"Nombre de lignes: {len(pandas_df)}")
    
    spark.stop() 


if __name__ == "__main__": 
    main()