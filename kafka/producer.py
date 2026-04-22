import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker('id_ID')

producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
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

def generate_user_event():
    product = random.choice(PRODUCTS)
    return {
        "event_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "event_type": random.choice(["view", "click", "add_to_cart", "remove_from_cart"]),
        "product_id": product["id"],
        "product_name": product["name"],
        "session_id": fake.uuid4(),
        "timestamp": datetime.now().isoformat(),
    }

def generate_payment():
    return {
        "payment_id": fake.uuid4(),
        "order_id": fake.uuid4(),
        "amount": random.randint(1, 5) * random.choice([p["price"] for p in PRODUCTS]),
        "method": random.choice(["transfer", "gopay", "ovo", "dana", "cod"]),
        "status": random.choice(["success", "failed", "pending"]),
        "paid_at": datetime.now().isoformat(),
    }

print("Producer mulai mengirim data ke semua topic...")

while True:
    order = generate_order()
    producer.send('orders', value=order)
    print(f"[ORDER]   {order['product_name']} | Rp{order['total_price']:,} | {order['status']}")

    event = generate_user_event()
    producer.send('user-events', value=event)
    print(f"[EVENT]   {event['event_type']} → {event['product_name']}")

    payment = generate_payment()
    producer.send('payments', value=payment)
    print(f"[PAYMENT] Rp{payment['amount']:,} | {payment['method']} | {payment['status']}")

    print("---")
    time.sleep(2)