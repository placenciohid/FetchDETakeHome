import json
import time
import uuid
import random
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()}')

def generate_data():
    user_id = str(uuid.uuid4())
    app_version = "2.3.0"
    device_type = random.choice(["android", "ios"])
    ip = f"{random.randint(100, 199)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
    locale = random.choice(["RU", "US", "IN", "CN"])
    device_id = f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
    timestamp = str(int(time.time()))
    
    return {
        "user_id": user_id,
        "app_version": app_version,
        "device_type": device_type,
        "ip": ip,
        "locale": locale,
        "device_id": device_id,
        "timestamp": timestamp
    }

def main():
    conf = {
        'bootstrap.servers': 'kafka:29092',
    }
    
    producer = Producer(conf)
    
    while True:
        data = generate_data()
        producer.produce('user-login', key=data['user_id'], value=json.dumps(data), callback=delivery_report)
        producer.flush()
        time.sleep(1)  # generate new data every second

if __name__ == "__main__":
    main()
