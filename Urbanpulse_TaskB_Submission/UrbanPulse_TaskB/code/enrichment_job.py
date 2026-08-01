from kafka import KafkaConsumer, KafkaProducer
import json
import csv

route_table ={}
with open('route_schedule.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        route_table[row['route_id']] = {
            "route_name": row['route_name'],
            "scheduled_arrival_time": row['scheduled_arrival_time'],
            "terminal": row['terminal']
        }

print(f"Loaded route_schedule KTable with {len(route_table)} routes: {list(route_table.keys())}")
consumer = KafkaConsumer(
    'urbanpulse.bus_gps',
    bootstrap_servers='localhost:9092',
    group_id ='enrichment-job-group',
    auto_offset_reset ='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Enrichment job started - joining bus_gps stream with route_schedule KTable...")
count = 0

for message in consumer:
    gps_event = message.value
    route_id = gps_event.get('route_id')

    schedule_info = route_table.get(route_id,{
        "route_name": "UNKNOWN",
        "scheduled_arrival_time": "N/A",
        "terminal": "N/A"
    })

    enriched_event = {
        **gps_event,
        "route_name": schedule_info["route_name"],
        "scheduled_arrival_time": schedule_info["scheduled_arrival_time"],
        "terminal": schedule_info["terminal"]
    }

    count += 1
    if count % 10 == 0:
        print(f"Enriched {count} events | Last: route={route_id} -> {schedule_info['route_name']}") 
