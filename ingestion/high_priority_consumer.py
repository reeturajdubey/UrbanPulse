from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    'urbanpulse.signals',
    bootstrap_servers='localhost:9092',
    group_id='HIGH_PRIORITY',
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("HIGH_PRIORITY consumer started - processing fast, no artificial delay.")
count = 0
for message in consumer:
    count += 1
    if count % 20 == 0:
        print(f"[HIGH_PRIORITY] Processed {count} messages | partition={message.partition} | junction={message.value.get('junction_id')}")
