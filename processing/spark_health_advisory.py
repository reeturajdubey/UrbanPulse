from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, window, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("UrbanPulse_Health_Advisory") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/health_advisory") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Load static zone profile
    # zone, population, number_of_schools
    zone_profile_schema = StructType([
        StructField("zone", StringType(), True),
        StructField("population", IntegerType(), True),
        StructField("number_of_schools", IntegerType(), True)
    ])
    
    static_zone_df = spark.read \
        .csv("data/zone_profile.csv", schema=zone_profile_schema, header=True)

    # Define schema for air_quality
    # sensor_id, zone, pm25, pm10, no2, aqi, timestamp
    aqi_schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("aqi", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Read from Kafka
    aqi_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "urbanpulse.air_quality") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON
    parsed_aqi_df = aqi_stream_df.select(
        from_json(col("value").cast("string"), aqi_schema).alias("data")
    ).select("data.*")

    # Convert timestamp to TimestampType for windowing
    with_time_df = parsed_aqi_df.withColumn("event_time", col("timestamp").cast("timestamp"))

    # Compute 10-minute rolling average AQI per zone
    # We use a sliding window: 10 minutes duration, sliding every 1 minute (as typical for rolling avg)
    rolling_avg_df = with_time_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            col("zone"),
            window(col("event_time"), "10 minutes", "1 minute")
        ) \
        .agg(avg("aqi").alias("rolling_avg_aqi"))

    # Join with static zone_profile
    enriched_df = rolling_avg_df.join(static_zone_df, "zone")

    # Filter for unhealthy AQI
    unhealthy_df = enriched_df.filter(col("rolling_avg_aqi") > 150)

    # Format for Kafka sink
    kafka_out_df = unhealthy_df.select(
        col("zone").alias("key"),
        to_json(struct(
            col("zone"),
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("rolling_avg_aqi"),
            col("population"),
            col("number_of_schools"),
            col("rolling_avg_aqi").alias("alert_level")
        )).alias("value")
    )

    # Write to urbanpulse.health_advisories in Update mode
    query = kafka_out_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "urbanpulse.health_advisories") \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
