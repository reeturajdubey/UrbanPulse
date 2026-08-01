# UrbanPulse: Complete Smart City Platform

This repository contains the complete codebase for the UrbanPulse smart city platform, including the data ingestion, messaging, and stream processing layers.

## Architecture: Hybrid Lambda-Inspired Kappa Platform ("Lambda-in-Practice")
The processing layer implements a hybrid architecture to fulfill the dual mandate of low-latency emergency operational response and long-term immutable state reporting:

### 1. Real-Time Speed Layer (Apache Flink)
Located in `processing/flink_incident_detector.py`
* Serves as a pure Kappa-style stream processor for immediate incident detection.
* Reads directly from Kafka.
* **Capabilities:** 
  * Sub-2-minute Air Quality (AQI) emergency alerts.
  * Traffic signal adaptation (Gridlock detection).
  * Stateful, geospatial bus bunching detection utilizing `KeyedState`.

### 2. Batch & Ward Analytics Layer (Apache Spark)
Located in `processing/spark_ward_analytics.py` and `processing/spark_health_advisory.py`
* Serves as the batch reporting mechanism.
* **Ingestion (Query 1):** Spark Structured Streaming reads from Kafka to write raw, append-only Parquet files into the object storage data lake (MinIO/Iceberg).
* **Analytics (Query 2):** Spark reads the Parquet data lake files to compute deterministic 15-minute tumbling windows for Ward Energy Aggregations, ensuring 100% audit reproducibility without degrading real-time pipelines.

## Project Structure
* `ingestion/` - Contains data ingestion scripts and Kafka producers.
* `processing/` - Contains the Flink and Spark streaming applications.
* `processing/mock_data_generator.py` - Script for simulating Kafka topics locally.
* `data/` - Static geospatial lookups (`zone_profile.csv`) and routing data.
* `report/` - Official documentation (including the Flink vs. Spark engine selection justification).
* `docker-compose.yml` - Infrastructure definition for Kafka, Flink, and Spark.