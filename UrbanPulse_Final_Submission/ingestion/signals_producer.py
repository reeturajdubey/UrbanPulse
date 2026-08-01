from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

JUNCTIONS = [f"JN-{i}" for i in range(1,11)]
ZONES = ['Zone-A', 'Zone-B', 'Zone-C', 'Zone-D']
PHASES = ['RED', 'GREEN', 'YELLOW']

def generate_signal_event(junction_id):
    return {
        "junction_id": junction_id,
        "zone": random.choice(ZONES),
        "vehicle_count": random.randint(5,150),
        "avg_wait_sec": round(random.uniform(10,220),1),
        "signal_phase": random.choice(PHASES),
        "timestamp": datetime.utcnow().isoformat()
    }

print("Starting signals producer... Ctrl+C to stop.")
event_count = 0

try:
   while True:
       junction = random.choice(JUNCTIONS)
       event = generate_signal_event(junction)
       producer.send('urbanpulse.signals', key=junction,value=event)

       event_count += 1
       if event_count % 20 == 0:
           print(f"Sent {event_count} events | Last: {event}")

       time.sleep(0.2)

except KeyboardInterrupt:
    print(f"\nStopped. Total events sent: {event_count}")
    producer.flush()
    producer.close()
