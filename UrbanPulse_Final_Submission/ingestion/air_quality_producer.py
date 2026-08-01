from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=5,
    retry_backoff_ms=300
)

SENSORS = [f"SENSOR-{i}" for i in range(1,21)]
ZONES = ['Zone-A','Zone-B','Zone-C','Zone-D']

def generate_aqi_event(sensor_id):
 
    is_null_event = random.random() < 0.05
    aqi_value = None if is_null_event else random.randint(20, 450)

    return {
         "sensor_id": sensor_id,
         "zone": random.choice(ZONES),
         "pm25": round(random.uniform(5,200),1),
         "pm10": round(random.uniform(10,300),1),
         "no2": round(random.uniform(5,100),1),
         "aqi": aqi_value,
         "timestamp": datetime.utcnow().isoformat()
   }

def on_send_success(record_metadata):
    pass

def on_send_error(excp):
    print(f"ERROR: Failed to send message - {excp}")

print("Starting air_quality producer (with retry + null-AQI simulation)... Ctrl+C to stop.")
event_count = 0
null_count = 0

try:
   while True:
       sensor = random.choice(SENSORS)
       event = generate_aqi_event(sensor)

       if event['aqi'] is None:
           null_count += 1
           print(f"[WARNING] Null AQI generated for {sensor} - will be caught by DLQ validation downstream")

       future = producer.send('urbanpulse.air_quality', key=sensor, value=event)
       future.add_callback(on_send_success)
       future.add_errback(on_send_error)

       event_count += 1
       if event_count % 10 == 0:
           print(f"Sent {event_count} events (null AQI so far: {null_count}) | Last: {event}")

       time.sleep(0.5)

except KeyboardInterrupt:
    print(f"\nStopped. Total: {event_count} events, {null_count} null-AQI events ({round(null_count/event_count*100,1)}%)")
    producer.flush()
    producer.close()

