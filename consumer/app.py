import time
import os
import json
import logging
from kafka3 import KafkaConsumer, KafkaProducer

logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)
logger.setLevel(level)

print("Consumer starting up")
time.sleep(20)  # Wait for Kafka to start

poll_timeout_seconds = 5

kafka_topic_name = os.getenv("KAFKA_TOPIC")
kafka_pubtopic_name = os.getenv("KAFKA_PUBTOPIC")

consumer = KafkaConsumer(bootstrap_servers="kafka:9092", group_id='my-consumer-1')
consumer.subscribe(topics=[kafka_topic_name])

producer = KafkaProducer(bootstrap_servers="kafka:9092")

while True:
     
    msg = consumer.poll(timeout_ms=poll_timeout_seconds*1000) # wait 10 seconds for messages 

    if msg:
        
        # load notifications from all partitions
        notifications = [ json.loads( n.value ) for partition in msg.keys() for n in msg[partition]]
        
        initial_length = len(notifications)
        logger.info(f"{initial_length} new messages")


        # only accept notifications with a data_id
        notifications = [n for n in notifications if "data_id" in n.get('properties',{})]
    
        nr_empty = initial_length - len(notifications)
        logger.info("filtered out %s empty records inside one batch ", nr_empty )

        # filter out possible duplicates inside the batch
        data_ids_in_notifications = [n["properties"]["data_id"] for n in notifications]
        notifications = [n for i,n in enumerate(notifications) if not n["properties"]["data_id"] in data_ids_in_notifications[:i] ]
        nr_duplicate_in_batch = (initial_length+nr_empty)-len(notifications)
        logger.debug("filtered out %s duplicate records inside one batch ", nr_duplicate_in_batch )

        for n in notifications:
            try:
                producer.send(
                    topic=kafka_pubtopic_name,
                    value=json.dumps(n).encode("utf-8"),
                    key=n["properties"]["data_id"].encode("utf-8")
                )
            except Exception as e:
                logger.error("could not publish records to Kafka",exc_info=True)
                continue

    else:
        logger.debug("No new messages")
        time.sleep(2)


