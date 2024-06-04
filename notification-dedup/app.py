import time
import os
import json
import logging
import signal

from confluent_kafka import Producer, Consumer
from jsonschema import Draft202012Validator
from prometheus_client import start_http_server, Counter

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
kafka_error_topic = os.getenv("KAFKA_ERROR_TOPIC")

#consumer = KafkaConsumer(bootstrap_servers=kafka_broker, group_id='my-consumer-1')
consumer = Consumer({'bootstrap.servers': kafka_broker,
    'group.id': 'my-consumer-notification-1',
    'auto.offset.reset': 'earliest'})
logging.info(f"subscribing to {kafka_topic_name}")
consumer.subscribe([kafka_topic_name])

producer = Producer({'bootstrap.servers': kafka_broker})
logging.info("created producer")

NR_INVALID_MESSAGES = Counter('invalid_messages_total', 'Number of messages with invalid notification schema')
NR_DUPLICATES = Counter('duplicate_messages_total', 'Number of duplicate messages')
NR_KAFKA_PUB_ERRORS = Counter('kafka_publish_errors_total', 'Number of kafka publish errors')

t = start_http_server(int(os.getenv("METRIC_PORT", "8000")))

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        NR_KAFKA_PUB_ERRORS.inc()
        logging.debug('Message delivered to %s [%s]', msg.topic() , msg.partition())

DONE = False

def shutdown_gracefully(signum, stackframe):
    logging.info("shutdown initiated")
    global DONE
    DONE = True

signal.signal(signal.SIGINT, shutdown_gracefully)
signal.signal(signal.SIGTERM, shutdown_gracefully)

while not DONE:
     
    #msg = consumer.poll(timeout_ms=poll_timeout_seconds*1000) # wait for messages 

    messages = consumer.consume(num_messages=poll_batch_size, timeout=poll_timeout_seconds)

    if len(messages)>0:

        error_messages = []
        
        # load notifications from all partitions
        #notifications = [ json.loads( n.value ) for partition in msg.keys() for n in msg[partition]]
        notifications = [ json.loads( message.value()) for message in messages if not message.error() ]

        initial_length = len(notifications)
        logging.debug(f"{initial_length} new messages")

        # only accept valid notificatons 
        notifications = [n for n in notifications if draft_202012_validator.is_valid(n)]
        nr_non_valid = initial_length - len(notifications)
        if nr_non_valid>0:
            logging.warning("filtered out %s non-valid records inside one batch ", nr_non_valid )
            NR_INVALID_MESSAGES.inc(nr_non_valid)
            # add non-valid messages to error list
            for n in [n for n in notifications if not draft_202012_validator.is_valid(n)]:
                error_messages.append({"reason" : "non valid" , "data" : n })

        # filter out possible duplicates inside the batch
        data_ids_in_notifications = [n["properties"]["data_id"] for n in notifications]
        notifications = [n for i,n in enumerate(notifications) if not n["properties"]["data_id"] in data_ids_in_notifications[:i] ]
        nr_duplicate_in_batch = (initial_length+nr_non_valid)-len(notifications)
        if nr_duplicate_in_batch>0:
            NR_DUPLICATES.inc(nr_duplicate_in_batch)
            logging.debug("filtered out %s duplicate records inside one batch ", nr_duplicate_in_batch )

        for notification in notifications:
            producer.produce(
                topic=kafka_pubtopic_name,
                value=json.dumps(notification),
                key=notification["properties"]["data_id"],
                callback=delivery_report
            )

        if len(error_messages)>0:
            logging.warning("publishing %s error messages to %s", len(error_messages), kafka_error_topic )
            for error_message in error_messages:
                producer.produce(
                    topic=kafka_error_topic,
                    value=json.dumps(error_message),
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


logging.info("closing consumer")
consumer.close()


