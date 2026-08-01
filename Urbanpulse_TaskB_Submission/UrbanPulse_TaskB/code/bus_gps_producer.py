from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Producer setup - JSON serialize 
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ROUTES = ['R101','R102','R103','R104','R105']
BASE_LAT, BASE_LON = 19.0760, 72.8777

def generate_bus_event(route_id):
    return {
         "bus_id": f"BUS-{random.randint(1000, 9999)}",
         "route_id": route_id,
         "lat": round(BASE_LAT + random.uniform(-0.05, 0.05), 6),
         "lon": round(BASE_LON + random.uniform(-0.05, 0.05), 6),
         "speed_kmh": round(random.uniform(0, 60), 1),
         "occupancy_pct": random.randint(10,100),
         "timestamp": datetime.utcnow().isoformat()
    }

print("Starting bus_gps producer... Ctrl+C to stop ")
event_count = 0

try:
   while True:
       route =  random.choice(ROUTES)
       event = generate_bus_event(route)

       producer.send('urbanpulse.bus_gps', key=route, value=event)

       event_count += 1
       if event_count % 10 == 0:
           print(f"Sent {event_count} events | Last: {event}")
      
       time.sleep(0.5)

except KeyboardInterrupt:
     print(f"\nStopped. Total events sent: {event_count}")
     producer.flush()
     producer.close()
