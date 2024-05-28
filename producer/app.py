import time
from kafka3 import KafkaProducer

print("Producer starting up")
time.sleep(20)  # Wait for Kafka to start

producer = KafkaProducer(bootstrap_servers="kafka:9092")
count=1
while True:
    message = f"Message: {count}"
    producer.send(
        topic="notifications",
        value=message.encode("utf-8"),
        key
        
    )
    if count%20==0:
        print(f"Sent: {message}")
    count = count+1

    time.sleep(0.05)