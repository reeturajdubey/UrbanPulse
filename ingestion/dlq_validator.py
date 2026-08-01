from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime 
from collections import Counter

consumer = KafkaConsumer(
     'urbanpulse.air_quality',
     bootstrap_servers='localhost:9092',
     auto_offset_reset='earliest',
     group_id='dlq-validator-group',
     value_deserializer=lambda v:json.loads(v.decode('utf-8')),
     consumer_timeout_ms=15000
)

dlq_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def validate(record):
    if record.get('aqi') is None:
        return "NULL_AQI_VALUE"
    if not (0 <= record['aqi'] <= 500):
        return "AQI_OUT_OF_RANGE"
    return None

error_counter = Counter()
total_processed = 0
total_failed = 0

print("Starting DLQ validator.. reading urbanpulse.air_quality (will auto-stop after 15s idle)")

for message in consumer:
    record = message.value
    total_processed += 1
    error_reason =validate(record)

    if error_reason:
        total_failed += 1
        error_counter[error_reason] += 1

        dlq_record = {
            "original_payload": record,
            "error_reason": error_reason,
            "source_topic": "urbanpulse.air_quality",
            "failed_at": datetime.utcnow().isoformat()
        }
        dlq_producer.send('urbanpulse.dlq', value=dlq_record)
        print(f"[DLQ] Routed record from {record.get('sensor_id')} - reason: {error_reason}")

dlq_producer.flush()

print(f"\n--- 5-Minute DLQ Report (simulated on {total_processed} messages) ---")
print(f"Total processed: {total_processed}")
print(f"Total failed/routed to DLQ: {total_failed} ({round(total_failed/total_processed*100,1) if total_processed else 0}%)")
print("Error type distribution:")
for reason, count in error_counter.items():
    print(f"  {reason}: {count}")
