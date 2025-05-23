from kafka import KafkaProducer
import time
import random
import json

# Kafka config
KAFKA_TOPIC = "system.logs"
KAFKA_BROKER = "localhost:9093"  # or 29092 depending on your docker-compose setup

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Sample logs
LOG_EVENTS = [
    "CPU spike detected",
    "Database restarted",
    "5 login failures in 10 minutes",
    "Auth service timeout",
    "Disk space warning",
    "Network latency increased",
    "Application error 500",
    "Memory usage exceeded 85%",
    "Security audit started",
    "Backup completed successfully"
]

print(f"Producing logs to topic '{KAFKA_TOPIC}'... Press Ctrl+C to stop.\n")

try:
    while True:
        log_entry = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "message": random.choice(LOG_EVENTS)
        }
        producer.send(KAFKA_TOPIC, log_entry)
        print(f"[Sent] {log_entry}")
        time.sleep(2)  # simulate 1 log every 2 seconds
except KeyboardInterrupt:
    print("\nLog generation stopped.")
    producer.close()
