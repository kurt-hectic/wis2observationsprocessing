import time
from kafka3 import KafkaConsumer

print("Consumer starting up")
time.sleep(20)  # Wait for Kafka to start

poll_timeout_seconds = 5

consumer = KafkaConsumer(bootstrap_servers="kafka:9092", group_id='my-consumer-1')
consumer.subscribe(topics=["notifications"])

while True:
     
    msg = consumer.poll(timeout_ms=poll_timeout_seconds*1000) # wait 10 seconds for messages 

    if msg:
        print(f"New messages: {msg}")
    else:
        print("No new messages")

    time.sleep(2)

