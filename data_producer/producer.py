import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

# ---------------- Kafka Producer ----------------
producer  = KafkaProducer(
       bootstrap_servers=['localhost:9092'],
       value_serializer = lambda v: json.dumps(v).encode("utf-8")


)

# ---------------- Mock Catalog & Users ----------------
product_catalog = [
    {"product_id": "P001", "name": "Smartphone", "category": "Electronics", "price": 799.99},
    {"product_id": "P002", "name": "Laptop", "category": "Electronics", "price": 1199.99},
    {"product_id": "P003", "name": "T-Shirt", "category": "Apparel", "price": 19.99},
    {"product_id": "P004", "name": "Jeans", "category": "Apparel", "price": 39.99},
    {"product_id": "P005", "name": "Blender", "category": "Home & Garden", "price": 89.99},
    {"product_id": "P006", "name": "Yoga Mat", "category": "Sports", "price": 29.99}
]

regions = ["US-East", "US-West", "IN-North", "IN-South", "EU-Central"]

payment_methods = ["card", "upi", "wallet", "cod"]

# ---------------- Event Generators ----------------
def generate_user_event():
    user_id = f"U{random.randint(1000, 9999)}"
    return {
        "event_type": "user_signup",
        "user_id": user_id,
        "email": f"{user_id.lower()}@example.com",
        "region": random.choice(regions),
        "created_at": datetime.utcnow().isoformat()
    }

def generate_order_event():
    order_id = str(uuid.uuid4())[:8]
    user_id = f"U{random.randint(1000, 9999)}"
    product = random.choice(product_catalog)
    qty = random.randint(1, 3)
    total = round(product["price"] * qty, 2)
    return {
        "event_type": "order_placed",
        "order_id": order_id,
        "user_id": user_id,
        "items": [{"product_id": product["product_id"], "qty": qty}],
        "total": total,
        "payment_method": random.choice(payment_methods),
        "discount_code": random.choice([None, "WELCOME10", "FREESHIP", ""]),
        "region": random.choice(regions),
        "created_at": datetime.utcnow().isoformat()
    }

def generate_payment_event(order_id, user_id, amount):
    return {
        "event_type": random.choice(["payment_success", "payment_failed"]),
        "order_id": order_id,
        "user_id": user_id,
        "amount": amount,
        "method": random.choice(payment_methods),
        "paid_at": datetime.utcnow().isoformat()
    }

def generate_shipment_event(order_id):
    return {
        "event_type": random.choice(["shipment_initiated", "delivered", "refund_requested"]),
        "order_id": order_id,
        "carrier": random.choice(["DHL", "FedEx", "BlueDart"]),
        "tracking_id": str(uuid.uuid4())[:12],
        "shipped_at": datetime.utcnow().isoformat()
    }

# ---------------- Main Loop ----------------
if __name__ == "__main__":
    print("Producing events to Kafka... Press Ctrl+C to stop.")
    try:
        while True:
            choice = random.choice(["users", "orders", "payments", "shipments"])

            if choice == "users":
                event = generate_user_event()
                producer.send("users", value=event)

            elif choice == "orders":
                order_event = generate_order_event()
                producer.send("orders", value=order_event)

                # Also create a payment event for this order
                payment_event = generate_payment_event(
                    order_event["order_id"],
                    order_event["user_id"],
                    order_event["total"]
                )
                producer.send("payments", value=payment_event)

                # Maybe create a shipment event if payment succeeded
                if payment_event["event_type"] == "payment_success":
                    shipment_event = generate_shipment_event(order_event["order_id"])
                    producer.send("shipments", value=shipment_event)

            elif choice == "payments":
                # Random standalone payment (rare case, e.g., retry)
                order_id = str(uuid.uuid4())[:8]
                event = generate_payment_event(order_id, f"U{random.randint(1000, 9999)}", random.uniform(10, 500))
                producer.send("payments", value=event)

            elif choice == "shipments":
                order_id = str(uuid.uuid4())[:8]
                event = generate_shipment_event(order_id)
                producer.send("shipments", value=event)

            producer.flush()
            time.sleep(random.uniform(0.1, 0.5))  # simulate irregular traffic

    except KeyboardInterrupt:
        print("\nStopped producing events.")
        producer.close()
