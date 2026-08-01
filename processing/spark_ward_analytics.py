from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum, avg, max, window, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def main():
    spark = SparkSession.builder \
        .appName("UrbanPulse_Ward_Analytics_Lambda") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/ward_analytics") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # meter_id, ward_id, kwh_reading, voltage, power_factor, timestamp (epoch ms)
    schema = StructType([
        StructField("meter_id", StringType(), True),
        StructField("ward_id", StringType(), True),
        StructField("kwh_reading", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("power_factor", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])

    # =========================================================================
    # PART 1: INGESTION (Kafka -> Parquet data lake)
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

    enriched_df = parsed_df.withColumn("event_time", (col("timestamp") / 1000).cast("timestamp")) \
                           .withColumn("date", to_date(col("event_time")))

    ingestion_query = enriched_df.writeStream \
        .format("parquet") \
        .option("path", "data/parquet/smart_meters_raw/") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/raw_ingestion") \
        .partitionBy("date", "ward_id") \
        .outputMode("append") \
        .start()

    # Local demo: show incoming smart-meter rows
    console_query = enriched_df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    # =========================================================================
    # PART 2: ANALYTICS (Kafka stream -> ward aggregations)
    # Assignment target is 15-minute tumbling windows; for a live demo we use
    # 1-minute windows so councillor-style summaries appear quickly.
    # =========================================================================
    ward_aggregations = enriched_df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            col("ward_id"),
            window(col("event_time"), "1 minute")
        ) \
        .agg(
            sum("kwh_reading").alias("total_kwh_consumed"),
            avg("power_factor").alias("avg_power_factor"),
            max("voltage").alias("peak_voltage")
        )

    def write_to_postgres_mock(batch_df, batch_id):
        # Mock PostgreSQL JDBC sink via console for the assignment
        print(f"--- Writing Batch {batch_id} to PostgreSQL OLAP Cache ---")
        batch_df.orderBy("ward_id").show(truncate=False)

    analytics_query = ward_aggregations.writeStream \
        .foreachBatch(write_to_postgres_mock) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/ward_agg") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
