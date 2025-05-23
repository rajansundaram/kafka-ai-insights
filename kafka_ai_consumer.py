from kafka import KafkaConsumer
import json
import time
import os
import openai
from dotenv import load_dotenv

# Load env vars
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

# Kafka config
KAFKA_TOPIC = "system.logs"
KAFKA_BROKER = "localhost:9093"

# Set up consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='gpt-log-summarizer'
)

print("[Consumer] Listening for log messages...\n")

# Buffer logs before sending to GPT
log_batch = []
BATCH_SIZE = 5

for message in consumer:
    log = message.value
    log_batch.append(log['message'])
    print(f"[Received] {log['message']}")

    if len(log_batch) >= BATCH_SIZE:
        print("\n[Summarizing log batch with GPT...]")
        prompt = "Summarize the following system logs in plain language:\n" + "\n".join(log_batch)

        try:
            client = openai.OpenAI(api_key=openai.api_key)
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}]
            )
            summary = response.choices[0].message.content
            print("\n[Summary]:\n", summary, "\n")
        except Exception as e:
            print(f"[ERROR] GPT summarization failed: {e}")

        log_batch.clear()
