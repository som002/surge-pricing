#!/usr/bin/env python3
"""
Data generator for Project 21: Ride-Sharing Surge Pricing Signals.
Publishes ride-sharing events to a Kafka topic.
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta
import os

from faker import Faker
from kafka import KafkaProducer

fake = Faker()

ZONES = [f"ZONE-{chr(65+i)}{j}" for i in range(5) for j in range(1, 6)]
EVENT_TYPES = ["ride_request", "driver_available", "ride_accepted", "ride_started", "ride_completed"]

# NYC-ish coordinates
LAT_BASE, LON_BASE = 40.75, -73.98


def generate_record():
    """Generate a single ride-sharing event."""
    zone = random.choice(ZONES)
    zone_row = ord(zone.split("-")[1][0]) - 65
    zone_col = int(zone.split("-")[1][1]) - 1
    lat = LAT_BASE + zone_row * 0.01 + random.uniform(-0.005, 0.005)
    lon = LON_BASE + zone_col * 0.01 + random.uniform(-0.005, 0.005)

    event_time = datetime.utcnow()
    if random.random() < 0.10:
        event_time -= timedelta(minutes=random.uniform(1, 5))

    # Simulate rush hour: more requests than drivers
    if random.random() < 0.55:
        evt = "ride_request"
        rider_id = f"RDR-{random.randint(10000, 99999)}"
        driver_id = None
        dlat = round(lat + random.uniform(-0.03, 0.03), 6)
        dlon = round(lon + random.uniform(-0.03, 0.03), 6)
    elif random.random() < 0.65:
        evt = "driver_available"
        rider_id = None
        driver_id = f"DRV-{random.randint(1000, 9999)}"
        dlat = None
        dlon = None
    else:
        evt = random.choice(["ride_accepted", "ride_started", "ride_completed"])
        rider_id = f"RDR-{random.randint(10000, 99999)}"
        driver_id = f"DRV-{random.randint(1000, 9999)}"
        dlat = round(lat + random.uniform(-0.03, 0.03), 6)
        dlon = round(lon + random.uniform(-0.03, 0.03), 6)

    return {
        "request_id": f"REQ-{uuid.uuid4().hex[:10].upper()}",
        "rider_id": rider_id,
        "driver_id": driver_id,
        "event_type": evt,
        "pickup_lat": round(lat, 6),
        "pickup_lon": round(lon, 6),
        "dropoff_lat": dlat,
        "dropoff_lon": dlon,
        "zone_id": zone,
        "event_time": event_time.isoformat()
    }


def main():
    parser = argparse.ArgumentParser(description="Ride-sharing event data generator")
    parser.add_argument("--bootstrap-servers", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="topic1", help="Kafka topic to publish to (default: topic1)")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds (default: 60)")
    parser.add_argument("--rate", type=float, default=5, help="Messages per second (default: 5)")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    print(f"Connected to Kafka at {args.bootstrap_servers}")
    print(f"Publishing to topic '{args.topic}' at {args.rate} msgs/sec for {args.duration}s")
    print("\nSample records:")

    start = time.time()
    count = 0
    recent = []

    try:
        while time.time() - start < args.duration:
            record = generate_record()

            if random.random() < 0.05 and recent:
                record = random.choice(recent).copy()
            else:
                recent.append(record)
                if len(recent) > 20:
                    recent.pop(0)

            producer.send(args.topic, value=record)
            count += 1

            if count <= 3:
                print(json.dumps(record, indent=2))

            time.sleep(1.0 / args.rate)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        producer.flush()
        producer.close()

    print(f"\nPublished {count} messages in {args.duration} seconds.")


if __name__ == "__main__":
    main()
