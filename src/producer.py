import time 
import json
import pandas as pd
from kafka import KafkaProducer

# Kafka config
TOPIC = "events"
BOOTSTRAP_SERVERS = "localhost:9092"

# load dữ liệu 
events = pd.read_csv("data/processed/event.csv")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

# sending events function
def send_events():
    for _, row in events.iterrows():
        event_message = {
            "customer_id": row['customer_id'],
            "product_id": row['product_id'],
            "event_name": row['event_name'],
            "event_time": row['event_time'],
            "quantity": row['quantity'],
        }
        producer.send(topic=TOPIC, value=event_message)
        print(f"Sent: {event_message}")
        time.sleep(0.5) # giả lập stream real time 0.5s/event
        
if __name__== "__main__":
    send_events()