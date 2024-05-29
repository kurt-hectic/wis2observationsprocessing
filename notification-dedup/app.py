import time
import os
import json
import logging

from confluent_kafka import Producer, Consumer
from jsonschema import Draft202012Validator

log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )

schema = json.loads(open("wis2-notification-message-bundled.json").read())
Draft202012Validator.check_schema(schema)
draft_202012_validator = Draft202012Validator(schema)

poll_timeout_seconds = int(os.getenv("POLL_TIMEOUT_SEC"))
poll_batch_size = int(os.getenv("POLL_BATCH_SIZE"))

kafka_broker = os.getenv("KAFKA_BROKER")
kafka_topic_name = os.getenv("KAFKA_TOPIC")
kafka_pubtopic_name = os.getenv("KAFKA_PUBTOPIC")
kafka_broker = os.getenv("KAFKA_BROKER")

#consumer = KafkaConsumer(bootstrap_servers=kafka_broker, group_id='my-consumer-1')
consumer = Consumer({'bootstrap.servers': kafka_broker,
    'group.id': 'my-consumer-notification-1',
    'auto.offset.reset': 'earliest'})
logging.info(f"subscribing to {kafka_topic_name}")
consumer.subscribe([kafka_topic_name])

producer = Producer({'bootstrap.servers': kafka_broker})
logging.info("created producer")

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.debug('Message delivered to %s [%s]', msg.topic() , msg.partition())



while True:
     
    #msg = consumer.poll(timeout_ms=poll_timeout_seconds*1000) # wait for messages 

    messages = consumer.consume(num_messages=poll_batch_size, timeout=poll_timeout_seconds)

    if len(messages)>0:
        
        # load notifications from all partitions
        #notifications = [ json.loads( n.value ) for partition in msg.keys() for n in msg[partition]]
        notifications = [ json.loads( message.value()) for message in messages if not message.error() ]

        initial_length = len(notifications)
        logging.info(f"{initial_length} new messages")

        # only accept valid notificatons 
        notifications = [n for n in notifications if draft_202012_validator.is_valid(n)]
        nr_non_valid = initial_length - len(notifications)
        logging.info("filtered out %s non-valid records inside one batch ", nr_non_valid )

        # filter out possible duplicates inside the batch
        data_ids_in_notifications = [n["properties"]["data_id"] for n in notifications]
        notifications = [n for i,n in enumerate(notifications) if not n["properties"]["data_id"] in data_ids_in_notifications[:i] ]
        nr_duplicate_in_batch = (initial_length+nr_non_valid)-len(notifications)
        logging.info("filtered out %s duplicate records inside one batch ", nr_duplicate_in_batch )

        for notification in notifications:
            producer.produce(
                topic=kafka_pubtopic_name,
                value=json.dumps(notification),
                key=notification["properties"]["data_id"],
                callback=delivery_report
            )
        
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
        #time.sleep(poll_timeout_seconds)

    else:
        logging.debug("No new messages")


