import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker('id_ID')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PRODUCTS = [
    {"id": "P001", "name": "Sepatu Lari", "price": 450000},
    {"id": "P002", "name": "Kaos Polos", "price": 85000},
    {"id": "P003", "name": "Celana Jeans", "price": 320000},
    {"id": "P004", "name": "Jaket Hoodie", "price": 275000},
    {"id": "P005", "name": "Topi Baseball", "price": 95000},
]

def generate_order():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        "order_id": fake.uuid4(),
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "product_id": product["id"],
        "product_name": product["name"],
        "quantity": quantity,
        "total_price": product["price"] * quantity,
        "status": random.choice(["pending", "confirmed", "shipped"]),
        "ordered_at": datetime.now().isoformat(),
    }

print("Producer mulai mengirim order ke Kafka...")

while True:
    order = generate_order()
    producer.send('orders', value=order)
    print(f"Terkirim: {order['order_id']} | {order['product_name']} | Rp{order['total_price']:,}")
    time.sleep(2)