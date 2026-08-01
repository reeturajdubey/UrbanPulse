# UrbanPulse: Complete Project Walkthrough

This document provides a step-by-step guide to running the complete UrbanPulse smart city platform on your local machine. The architecture consists of Kafka for message brokering, Python scripts for data ingestion, and Apache Flink and Apache Spark for real-time and batch stream processing.

## 1. Prerequisites
- **Docker & Docker Compose**: Required for running ZooKeeper and Kafka locally.
- **Python 3.8+**: Ensure you have Python installed.
- **Java 8 or 11**: Required by Apache Spark and Apache Flink for the processing layer.

You will also need to install the necessary Python dependencies. (It is recommended to use a virtual environment).
```bash
pip install confluent-kafka apache-flink pyspark
```

## 2. Infrastructure Setup (Kafka & ZooKeeper)
Start the message broker infrastructure using Docker Compose. This will spin up a single-node ZooKeeper and Kafka cluster.

```bash
# From the root of the project
docker-compose up -d
```
Verify the containers are running:
```bash
docker ps
```
Kafka will be accessible at `localhost:9092`.

## 3. Data Ingestion Layer (Task A & B)
The ingestion layer is responsible for producing smart city data into Kafka topics. You can run individual producers depending on the data stream you wish to simulate.

In separate terminal windows, run the following producers:
```bash
# 1. Start Air Quality Data Ingestion
python ingestion/air_quality_producer.py

# 2. Start Bus GPS Data Ingestion
python ingestion/bus_gps_producer.py

# 3. Start Traffic Signals Data Ingestion
python ingestion/signals_producer.py
```
*(Note: Alternatively, you can run `python processing/mock_data_generator.py` to simulate all topics simultaneously).*

You can also run the consumers or enrichment jobs to view the data flowing through Kafka:
```bash
python ingestion/enrichment_job.py
python ingestion/high_priority_consumer.py
python ingestion/standard_priority_consumer.py
```

## 4. Stream Processing Layer (Task C)
The processing layer applies the Lambda-inspired Kappa architecture to fulfill real-time alerting and historical batch processing.

### Real-Time Speed Layer (Apache Flink)
Flink reads directly from Kafka for immediate incident detection (sub-2-minute SLA) and stateful alerts (e.g., Gridlock, Bus Bunching).

In a new terminal window, run:
```bash
python processing/flink_incident_detector.py
```
*This will subscribe to the Kafka topics and output real-time alerts to the console.*

### Batch & Analytics Layer (Apache Spark)
Spark Structured Streaming handles the ingestion into a Parquet data lake and computes deterministic tumbling window aggregations (e.g., Ward Energy Analytics).

In a new terminal window, run:
```bash
# Run Ward Analytics (15-min tumbling windows)
python processing/spark_ward_analytics.py

# Run Health Advisory Aggregations
python processing/spark_health_advisory.py
```
*Spark will read the data and write append-only analytics output, ensuring 100% audit reproducibility.*

## 5. Shutting Down
To gracefully stop the platform:
1. Stop all running Python scripts using `Ctrl+C`.
2. Spin down the Kafka and ZooKeeper containers:
```bash
docker-compose down
```
