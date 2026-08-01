from kafka import KafkaConsumer
import json
import time
import sys

consumer_id = sys.argv[1] if len(sys.argv) > 1 else "1"

consumer = KafkaConsumer(
    'urbanpulse.signals',
    bootstrap_servers='localhost:9092',
    group_id='STANDARD_PRIORITY',
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"STANDARD_PRIORITY consumer-{consumer_id} started - artificially slowed (0.5s per message).")
count = 0
for message in consumer:
    time.sleep(0.5)
    count += 1
    if count % 5 == 0:
        print(f"[STANDARD-{consumer_id}] Processed {count} messages | partition={message.partition}")
