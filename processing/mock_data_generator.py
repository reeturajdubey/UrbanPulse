import json
import time
import random
from confluent_kafka import Producer
from datetime import datetime

# Kafka configuration
conf = {'bootstrap.servers': 'localhost:9092'}

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        pass # print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def get_producer():
    try:
        return Producer(**conf)
    except Exception as e:
        print(f"Could not connect to Kafka: {e}. Outputting to console instead.")
        return None

def generate_bus_gps(anomaly_bunching=False):
    """
    Schema: bus_id, route_id, lat, lon, speed_kmh, occupancy_pct, timestamp
    """
    timestamp = int(time.time() * 1000)
    
    if anomaly_bunching:
        # Simulate bus bunching: 2 buses on same route, very close to each other
        route_id = "route_101"
        lat = 12.9716
        lon = 77.5946
        return [
            {"bus_id": "bus_001", "route_id": route_id, "lat": lat, "lon": lon, "speed_kmh": 15, "occupancy_pct": 80, "timestamp": timestamp},
            {"bus_id": "bus_002", "route_id": route_id, "lat": lat + 0.0001, "lon": lon + 0.0001, "speed_kmh": 14, "occupancy_pct": 40, "timestamp": timestamp}
        ]
        
    return [{
        "bus_id": f"bus_{random.randint(100, 999)}",
        "route_id": f"route_{random.randint(1, 50)}",
        "lat": round(random.uniform(12.8, 13.1), 4),
        "lon": round(random.uniform(77.4, 77.8), 4),
        "speed_kmh": random.randint(0, 60),
        "occupancy_pct": random.randint(10, 100),
        "timestamp": timestamp
    }]

def generate_signals(anomaly_gridlock=False, gridlock_count=0):
    """
    Schema: junction_id, zone, vehicle_count, avg_wait_sec, signal_phase, timestamp
    """
    timestamp = int(time.time() * 1000)
    zones = ["North", "South", "East", "West", "Central"]
    
    if anomaly_gridlock:
        return {
            "junction_id": "jnc_999",
            "zone": "Central",
            "vehicle_count": 150,
            "avg_wait_sec": 200, # > 180 for anomaly
            "signal_phase": "RED",
            "timestamp": timestamp
        }
        
    return {
        "junction_id": f"jnc_{random.randint(1, 100)}",
        "zone": random.choice(zones),
        "vehicle_count": random.randint(5, 50),
        "avg_wait_sec": random.randint(10, 120),
        "signal_phase": random.choice(["RED", "GREEN", "YELLOW"]),
        "timestamp": timestamp
    }

def generate_air_quality(anomaly_emergency=False):
    """
    Schema: sensor_id, zone, pm25, pm10, no2, aqi, timestamp
    """
    timestamp = int(time.time() * 1000)
    zones = ["North", "South", "East", "West", "Central"]
    
    if anomaly_emergency:
        return {
            "sensor_id": "sensor_alert_1",
            "zone": random.choice(zones),
            "pm25": 250.0,
            "pm10": 350.0,
            "no2": 150.0,
            "aqi": random.randint(301, 500), # > 300
            "timestamp": timestamp
        }
        
    return {
        "sensor_id": f"sensor_{random.randint(1, 20)}",
        "zone": random.choice(zones),
        "pm25": round(random.uniform(20.0, 100.0), 2),
        "pm10": round(random.uniform(40.0, 150.0), 2),
        "no2": round(random.uniform(10.0, 80.0), 2),
        "aqi": random.randint(30, 160),
        "timestamp": timestamp
    }

def generate_smart_meters():
    """
    Schema: meter_id, ward_id, kwh_reading, voltage, power_factor, timestamp
    """
    timestamp = int(time.time() * 1000)
    return {
        "meter_id": f"meter_{random.randint(1000, 9999)}",
        "ward_id": f"ward_{random.randint(1, 15)}",
        "kwh_reading": round(random.uniform(0.1, 5.0), 2),
        "voltage": round(random.uniform(210.0, 240.0), 1),
        "power_factor": round(random.uniform(0.8, 1.0), 2),
        "timestamp": timestamp
    }

def main():
    p = get_producer()
    print("Starting Mock Data Generator...")
    
    # State for testing gridlock
    gridlock_counter = 0
    
    while True:
        try:
            # 1. Bus GPS
            anomaly_bus = random.random() < 0.05
            buses = generate_bus_gps(anomaly_bunching=anomaly_bus)
            for bus in buses:
                val = json.dumps(bus)
                if p: p.produce('urbanpulse.bus_gps', key=bus['route_id'].encode(), value=val, callback=delivery_report)
                else: print(f"[BUS] {val}")
                
            # 2. Signals
            anomaly_gridlock = random.random() < 0.1
            if anomaly_gridlock: gridlock_counter += 1
            else: gridlock_counter = 0
            
            signal = generate_signals(anomaly_gridlock=(gridlock_counter > 0), gridlock_count=gridlock_counter)
            val = json.dumps(signal)
            if p: p.produce('urbanpulse.signals', key=signal['junction_id'].encode(), value=val, callback=delivery_report)
            else: print(f"[SIGNAL] {val}")
            
            # 3. Air Quality
            anomaly_aqi = random.random() < 0.05
            aqi = generate_air_quality(anomaly_emergency=anomaly_aqi)
            val = json.dumps(aqi)
            if p: p.produce('urbanpulse.air_quality', key=aqi['sensor_id'].encode(), value=val, callback=delivery_report)
            else: print(f"[AQI] {val}")
            
            # 4. Smart Meters
            meter = generate_smart_meters()
            val = json.dumps(meter)
            if p: p.produce('urbanpulse.smart_meters', key=meter['ward_id'].encode(), value=val, callback=delivery_report)
            else: print(f"[METER] {val}")

            if p: p.poll(0)
            time.sleep(1)
            
        except KeyboardInterrupt:
            break
            
    if p: p.flush()
    print("Stopped generator.")

if __name__ == '__main__':
    main()
