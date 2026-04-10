import json
import os
from datetime import datetime
from kafka import KafkaConsumer
from threading import Thread
import time

os.makedirs('data/raw/orders', exist_ok=True)
os.makedirs('data/raw/user-events', exist_ok=True)
os.makedirs('data/raw/payments', exist_ok=True)

def save_to_file(topic, data):
    date_str = datetime.now().strftime('%Y-%m-%d')
    filepath = f'data/raw/{topic}/{date_str}.jsonl'
    with open(filepath, 'a') as f:
        f.write(json.dumps(data) + '\n')

def consume_topic(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        group_id=f'glowcart-{topic}-consumer',
        consumer_timeout_ms=-1
    )
    print(f"[{topic}] consumer siap...")
    for message in consumer:
        data = message.value
        save_to_file(topic, data)
        print(f"[{topic}] disimpan: {list(data.values())[0]}")

topics = ['orders', 'user-events', 'payments']
threads = []

for topic in topics:
    t = Thread(target=consume_topic, args=(topic,), daemon=True)
    threads.append(t)
    t.start()

print("Semua consumer berjalan. Tekan Ctrl+C untuk berhenti.")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nConsumer dihentikan.")