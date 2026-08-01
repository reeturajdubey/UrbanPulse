import json
import math
import time
from datetime import datetime
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.serialization import SimpleStringSchema

KAFKA_BROKERS = "localhost:9092"

def haversine(lat1, lon1, lat2, lon2):
    # Earth radius in meters
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

class AQIAlertFunction(MapFunction):
    def map(self, value):
        data = json.loads(value)
        if data.get("aqi", 0) > 300:
            return json.dumps({
                "incident_type": "AQI_EMERGENCY",
                "sensor_id": data["sensor_id"],
                "zone": data["zone"],
                "aqi": data["aqi"],
                "timestamp": data["timestamp"]
            })
        return None

class GridlockDetector(KeyedProcessFunction):
    def __init__(self):
        self.count_state = None
        
    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("gridlock_count", Types.INT())
        self.count_state = runtime_context.get_state(descriptor)
        
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        data = json.loads(value)
        wait_sec = data.get("avg_wait_sec", 0)
        
        current_count = self.count_state.value()
        if current_count is None:
            current_count = 0
            
        if wait_sec > 180:
            current_count += 1
            self.count_state.update(current_count)
            if current_count >= 3:
                # Emit alert
                yield json.dumps({
                    "incident_type": "TRAFFIC_GRIDLOCK",
                    "junction_id": data["junction_id"],
                    "zone": data["zone"],
                    "timestamp": data["timestamp"],
                    "message": "Average wait time > 180s for 3 consecutive cycles"
                })
                # Reset after alert? The requirement doesn't specify, but reset to avoid spamming
                self.count_state.clear()
        else:
            self.count_state.clear()

class BusBunchingDetector(KeyedProcessFunction):
    def __init__(self):
        self.bus_state = None
        self.bunching_start = None
        
    def open(self, runtime_context):
        # Store latest position per bus_id: MapState[bus_id, dict]
        # In PyFlink, MapStateDescriptor takes key and value type info. 
        # Using string for both (value is json dumped dict) for simplicity.
        self.bus_state = runtime_context.get_map_state(MapStateDescriptor("bus_positions", Types.STRING(), Types.STRING()))
        self.bunching_start = runtime_context.get_map_state(MapStateDescriptor("bunching_start", Types.STRING(), Types.LONG()))
        
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        data = json.loads(value)
        bus_id = data["bus_id"]
        timestamp_str = data["timestamp"]
        
        if isinstance(timestamp_str, str):
            try:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                timestamp = int(dt.timestamp() * 1000)
            except:
                timestamp = int(time.time() * 1000)
        else:
            timestamp = timestamp_str

        
        self.bus_state.put(bus_id, json.dumps(data))
        
        # Check distance with all other buses on this route
        for other_bus_id, other_bus_json in self.bus_state.items():
            if other_bus_id == bus_id:
                continue
                
            other_bus = json.loads(other_bus_json)
            dist = haversine(data["lat"], data["lon"], other_bus["lat"], other_bus["lon"])
            
            pair_key = f"{min(bus_id, other_bus_id)}_{max(bus_id, other_bus_id)}"
            
            if dist <= 200:
                start_time = self.bunching_start.get(pair_key)
                if start_time is None:
                    self.bunching_start.put(pair_key, timestamp)
                else:
                    # check if > 5 minutes (300,000 ms)
                    if (timestamp - start_time) > 300000:
                        yield json.dumps({
                            "incident_type": "BUS_BUNCHING",
                            "route_id": data["route_id"],
                            "bus_1": bus_id,
                            "bus_2": other_bus_id,
                            "timestamp": timestamp,
                            "distance_m": round(dist, 2)
                        })
                        # reset to avoid spam
                        self.bunching_start.remove(pair_key)
            else:
                self.bunching_start.remove(pair_key)


def main():
    import sys
    from pathlib import Path
    from pyflink.common import Configuration
    config = Configuration()
    config.set_string("python.executable", sys.executable)
    config.set_string("python.client.executable", sys.executable)
    env = StreamExecutionEnvironment.get_execution_environment(config)
    jar_dir = Path(__file__).resolve().parent
    env.add_jars(
        jar_dir.joinpath("flink-connector-kafka.jar").as_uri(),
        jar_dir.joinpath("kafka-clients.jar").as_uri(),
    )
    
    # 1. AQI Emergency Pipeline
    aqi_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics("urbanpulse.air_quality") \
        .set_group_id("flink_incident_group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    def as_text(value):
        # PyFlink UDFs often cross the Java boundary as bytes
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8")
        return value

    aqi_stream = env.from_source(aqi_source, WatermarkStrategy.no_watermarks(), "AQI Source")
    aqi_alerts = aqi_stream.map(AQIAlertFunction(), output_type=Types.STRING()) \
        .filter(lambda x: x is not None) \
        .map(as_text, output_type=Types.STRING())
    
    # 2. Traffic Gridlock Pipeline
    signals_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics("urbanpulse.signals") \
        .set_group_id("flink_incident_group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    signals_stream = env.from_source(signals_source, WatermarkStrategy.no_watermarks(), "Signals Source")
    # Key by junction_id
    gridlock_alerts = signals_stream \
        .key_by(lambda x: json.loads(as_text(x))["junction_id"]) \
        .process(GridlockDetector(), output_type=Types.STRING()) \
        .map(as_text, output_type=Types.STRING())
        
    # 3. Bus Bunching Pipeline
    bus_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics("urbanpulse.bus_gps") \
        .set_group_id("flink_incident_group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    # We assign watermarks for event time processing if needed, 
    # but here we use simple processing logic inside KeyedProcessFunction.
    bus_stream = env.from_source(bus_source, WatermarkStrategy.no_watermarks(), "Bus Source")
    bunching_alerts = bus_stream \
        .key_by(lambda x: json.loads(as_text(x))["route_id"]) \
        .process(BusBunchingDetector(), output_type=Types.STRING()) \
        .map(as_text, output_type=Types.STRING())
        
    # 4. Sink to urbanpulse.incidents
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("urbanpulse.incidents")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
    
    # Console for local demo visibility; Kafka for downstream consumers
    aqi_alerts.print("AQI_ALERT")
    gridlock_alerts.print("GRIDLOCK_ALERT")
    bunching_alerts.print("BUNCHING_ALERT")

    aqi_alerts.sink_to(kafka_sink)
    gridlock_alerts.sink_to(kafka_sink)
    bunching_alerts.sink_to(kafka_sink)
    
    env.execute("UrbanPulse Flink Incident Detector")

if __name__ == '__main__':
    main()
