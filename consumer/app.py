import time
import os
import json
import logging
from kafka3 import KafkaConsumer, KafkaProducer

log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )

poll_timeout_seconds = int(os.getenv("POLL_TIMEOUT_SEC"))

kafka_topic_name = os.getenv("KAFKA_TOPIC")
kafka_pubtopic_name = os.getenv("KAFKA_PUBTOPIC")
kafka_broker = os.getenv("KAFKA_BROKER")

consumer = KafkaConsumer(bootstrap_servers=kafka_broker, group_id='my-consumer-1')
logging.info(f"subscribing to {kafka_topic_name}")
consumer.subscribe(topics=[kafka_topic_name])

#producer = KafkaProducer(bootstrap_servers="kafka:9092")

while True:
     
    msg = consumer.poll(timeout_ms=poll_timeout_seconds*1000) # wait for messages 

    if msg:
        
        # load notifications from all partitions
        notifications = [ json.loads( n.value ) for partition in msg.keys() for n in msg[partition]]
        
        initial_length = len(notifications)
        logging.info(f"{initial_length} new messages")


        # only accept notifications with a data_id
        notifications = [n for n in notifications if "data_id" in n.get('properties',{})]
    
        nr_empty = initial_length - len(notifications)
        logging.info("filtered out %s empty records inside one batch ", nr_empty )

        # filter out possible duplicates inside the batch
        data_ids_in_notifications = [n["properties"]["data_id"] for n in notifications]
        notifications = [n for i,n in enumerate(notifications) if not n["properties"]["data_id"] in data_ids_in_notifications[:i] ]
        nr_duplicate_in_batch = (initial_length+nr_empty)-len(notifications)
        logging.info("filtered out %s duplicate records inside one batch ", nr_duplicate_in_batch )

        # for n in notifications:
        #     try:
        #         producer.send(
        #             topic=kafka_pubtopic_name,
        #             value=json.dumps(n).encode("utf-8"),
        #             key=n["properties"]["data_id"].encode("utf-8")
        #         )
        #     except Exception as e:
        #         logger.error("could not publish records to Kafka",exc_info=True)
        #         continue
        time.sleep(poll_timeout_seconds)

    else:
        logging.debug("No new messages")
        time.sleep(2)


