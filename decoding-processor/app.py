import os
import json
import logging
import warnings
import base64
import signal

with warnings.catch_warnings():
    warnings.filterwarnings("ignore",message=r"ecCodes .* or higher is recommended")
    from bufr2geojson import __version__, transform as as_geojson

from confluent_kafka import Producer, Consumer
from prometheus_client import start_http_server, Counter, Summary


log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)
logging.getLogger("bufr2geojson").setLevel(logging.ERROR) # suppress bufr2geojson verbose output


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )


poll_timeout_seconds = int(os.getenv("POLL_TIMEOUT_SEC"))
poll_batch_size = int(os.getenv("POLL_BATCH_SIZE"))

kafka_broker = os.getenv("KAFKA_BROKER")
kafka_topic_name = os.getenv("KAFKA_TOPIC")
kafka_pubtopic_name = os.getenv("KAFKA_PUBTOPIC")
kafka_broker = os.getenv("KAFKA_BROKER")

t = start_http_server(int(os.getenv("METRIC_PORT", "8000")))
NR_DECODING_ERRORS = Counter('decoding_errors_total', 'Number of decoding errors')

#consumer = KafkaConsumer(bootstrap_servers=kafka_broker, group_id='my-consumer-1')
consumer = Consumer({'bootstrap.servers': kafka_broker,
    'group.id': 'my-consumer-decoding-1',
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


def decode_bufr(notification):
    ret = []
    try:
        geojson = as_geojson(base64.b64decode(notification["properties"]["content"]["value"]))
        
        if geojson and not geojson == None:
            for collection in geojson:
                for key, item in collection.items():
                    ret.append( item['geojson'] )

    except Exception as e:
        NR_DECODING_ERRORS.inc()
        logging.error("could not decode BUFR",exc_info=True)
        return []
    
    return ret


DONE = False

def shutdown_gracefully(signum, stackframe):
    logging.info("shutdown initiated")
    global DONE
    DONE = True

signal.signal(signal.SIGINT, shutdown_gracefully)
signal.signal(signal.SIGTERM, shutdown_gracefully)


while not DONE:
     
    messages = consumer.consume(num_messages=poll_batch_size, timeout=poll_timeout_seconds)

    if len(messages)>0:
        
        
        # load notifications from all partitions
        #notifications = [ json.loads( n.value ) for partition in msg.keys() for n in msg[partition]]
        notifications = [ json.loads( message.value()) for message in messages if not message.error() ]

        initial_length = len(notifications)
        logging.debug(f"{initial_length} new messages")

        observations = []

        for notification in notifications:
            collections = decode_bufr(notification)

            for collection in collections:
                observations.append({
                    "notification" : notification,
                    "data" : collection
                })



        logging.debug("sending %s observations to Kafka %s", len(observations),kafka_pubtopic_name)
        for o in observations:
            producer.produce(
                topic=kafka_pubtopic_name,
                value=json.dumps(o),
                callback=delivery_report
            )
        
    else:
        logging.debug("No new messages")


logging.info("closing consumer")
consumer.close()
