from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum, avg, max, window, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def main():
    spark = SparkSession.builder \
        .appName("UrbanPulse_Ward_Analytics_Lambda") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/ward_analytics") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Define schema for smart_meters
    # meter_id, ward_id, kwh_reading, voltage, power_factor, timestamp (ISO String from Task B)
    schema = StructType([
        StructField("meter_id", StringType(), True),
        StructField("ward_id", StringType(), True),
        StructField("kwh_reading", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("power_factor", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # =========================================================================
    # PART 1: INGESTION (Kafka -> Parquet/MinIO)
    # "Spark Structured Streaming reading from the same Kafka topics to write 
    # append-only Parquet files into on-premise object storage"
    # =========================================================================
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "urbanpulse.smart_meters") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Add event_time and date partitions
    enriched_df = parsed_df.withColumn("event_time", col("timestamp").cast("timestamp")) \
                           .withColumn("date", to_date(col("event_time")))

    # Query 1: Write raw streaming data append-only to Parquet
    ingestion_query = enriched_df.writeStream \
        .format("parquet") \
        .option("path", "data/parquet/smart_meters_raw/") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/raw_ingestion") \
        .partitionBy("date", "ward_id") \
        .outputMode("append") \
        .start()

    # =========================================================================
    # PART 2: ANALYTICS (Parquet -> PostgreSQL OLAP Cache)
    # "Spark reads these Parquet files for 15-minute ward aggregations and 
    # historical councillor reports."
    # =========================================================================
    
    # Read as a stream from the Parquet directory populated by Query 1
    # We define the schema explicitly to avoid needing data to infer
    parquet_schema = enriched_df.schema
    
    parquet_stream_df = spark.readStream \
        .schema(parquet_schema) \
        .parquet("data/parquet/smart_meters_raw/")

    # Apply 15-minute tumbling window with watermarks on the Parquet data
    ward_aggregations = parquet_stream_df \
        .withWatermark("event_time", "45 minutes") \
        .groupBy(
            col("ward_id"),
            window(col("event_time"), "15 minutes")
        ) \
        .agg(
            sum("kwh_reading").alias("total_kwh_consumed"),
            avg("power_factor").alias("avg_power_factor"),
            max("voltage").alias("peak_voltage")
        )

    # Query 2: Write aggregated results to PostgreSQL OLAP Cache
    # We mock the PostgreSQL JDBC sink using console for the assignment, 
    # as the DB is not provisioned in the docker-compose.
    def write_to_postgres_mock(batch_df, batch_id):
        # In a real environment: batch_df.write.jdbc(url="jdbc:postgresql://db:5432/urbanpulse", table="ward_summaries", ...)
        print(f"--- Writing Batch {batch_id} to PostgreSQL OLAP Cache ---")
        batch_df.show(truncate=False)

    analytics_query = ward_aggregations.writeStream \
        .foreachBatch(write_to_postgres_mock) \
        .outputMode("append") \
        .start()

    # Await termination for both streams running concurrently
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
