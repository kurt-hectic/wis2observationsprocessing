import os
import json
import logging
import requests, urllib
import base64, gzip, zlib
import hashlib
import signal

from confluent_kafka import Producer, Consumer
from prometheus_client import start_http_server, Counter, Summary

log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )


poll_timeout_seconds = int(os.getenv("POLL_TIMEOUT_SEC"))
poll_batch_size = int(os.getenv("POLL_BATCH_SIZE"))
nr_threads = int(os.getenv("NR_THREADS"))

kafka_broker = os.getenv("KAFKA_BROKER")
kafka_topic_name = os.getenv("KAFKA_TOPIC")
kafka_pubtopic_name = os.getenv("KAFKA_PUBTOPIC")
kafka_broker = os.getenv("KAFKA_BROKER")
kafka_error_topic = os.getenv("KAFKA_ERROR_TOPIC")

t = start_http_server(int(os.getenv("METRIC_PORT", "8000")))
NR_INTEGRITY_ERRORS = Counter('integrity_errors_total', 'Number of integrity errors')
NR_CONTENT_ERRORS = Counter('content_fetching_errors_total', 'Number of content fetching errors')
NR_KAFKA_PUB_ERRORS = Counter('kafka_publish_errors_total', 'Number of kafka publish errors')
DOWNLOAD_LATENCY = Summary('download_latency_seconds', 'Time spent downloading content')

#consumer = KafkaConsumer(bootstrap_servers=kafka_broker, group_id='my-consumer-1')
consumer = Consumer({'bootstrap.servers': kafka_broker,
    'group.id': 'my-consumer-content-1',
    'auto.offset.reset': 'earliest'})
logging.info(f"subscribing to {kafka_topic_name}")
consumer.subscribe([kafka_topic_name])

producer = Producer({'bootstrap.servers': kafka_broker})
logging.info("created producer")

session = requests.Session()
logging.info("created reuqests session")

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        NR_KAFKA_PUB_ERRORS.inc()
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.debug('Message delivered to %s [%s]', msg.topic() , msg.partition())



@NR_CONTENT_ERRORS.count_exceptions()
def handle_content(notification):
        url = [ l["href"] for l in notification["links"] if l["rel"] == "canonical" ][0]
        resp = session.request("GET",url, timeout=4)
        resp.raise_for_status()
        logging.debug("downloaded {} in {}".format(url,resp.elapsed))
        download_time=resp.elapsed.total_seconds()*1000
        parsed_url = urllib.parse.urlparse(url)
        cache = parsed_url.netloc

        DOWNLOAD_LATENCY.observe(download_time)

        return resp.content, download_time, cache

def decode_content(content):

    encoding_method =  content.get("encoding","base64").lower()

    if encoding_method == "base64":
        content_value = base64.b64decode(content["value"])
    elif encoding_method == "utf8" or encoding_method == "utf-8":
        content_value = content["value"].encode("utf8")
    elif encoding_method == "gzip":
        content_value = gzip.decompress(content["value"])
    else:
        raise Exception(f"encoding method {encoding_method} not supported")
    
    return content_value

def content_check(notification):
    if not "content" in notification["properties"]:
        try:
            content, download_time, cache = handle_content(notification)
        except requests.exceptions.HTTPError as e:
            logging.error(f"could not download content for {notification['properties']['data_id']} {e} ")
            return False
        except Exception as e:
            logging.error(f"could not download content for {notification['properties']['data_id']}")
            # TODO: process error in context of Kafka
            return False

        notification["properties"]["content"] = {
            "encoding": "base64",
            "value": base64.b64encode(content).decode("utf-8") ,
            "size": len(content)
        }

    return notification
    
ingegrity_methods =  [ "sha256", "sha384", "sha512", "sha3-256", "sha3-384", "sha3-512" ]

def integrity_check(notification):
    integrity_method =  notification["properties"]["integrity"]["method"].lower()

    if integrity_method not in ingegrity_methods:
        raise Exception(f"integrity method {integrity_method} not supported")

    content_bytes = decode_content(notification["properties"]["content"])
    
    if len(content_bytes) != notification["properties"]["content"]["size"]:
        raise Exception(f"content size mismatch for {notification['properties']['data_id']}")

    h = hashlib.new(integrity_method)
    h.update(content_bytes)
    checksum = base64.b64encode(h.digest()).decode("utf-8")

    original_checksum = notification["properties"]["integrity"]["value"] 

    if checksum != original_checksum:
        raise Exception(f"checksum mismatch for {notification['properties']['data_id']} ({checksum} vs {original_checksum})")
    
    return True

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

        # download content for each notification if not included
        notifications =  [ n for n in [ content_check(notification) for notification in notifications ] if n ]
        nr_with_content = len(notifications)
        logging.debug("number of notifications with content %s", nr_with_content)
        if initial_length-nr_with_content > 0:
            logging.warning("removed %s because of no content",initial_length-nr_with_content)

        # validate content checksum and length
        error_messages = []
        for i,n in enumerate(notifications):
            try:
                integrity_check(n)
            except Exception as e:
                error_messages.append({"reason" : "integrity error" , "detail" : str(e) , "data" : notifications.pop(i) })
                logging.error(f"integrity error for {n['properties']['data_id']} {e}")

        nr_with_ingegrity = len(notifications)
        logging.debug("number of notifications with correct integrity and length %s",nr_with_ingegrity)
        if len(error_messages)>0:
            NR_INTEGRITY_ERRORS.inc(len(error_messages))
            logging.warning("removed %s because of integrity or length",len(error_messages))


        # output

        logging.debug("sending %s notifications to Kafka %s", len(notifications),kafka_pubtopic_name)
        for notification in notifications:
            producer.produce(
                topic=kafka_pubtopic_name,
                value=json.dumps(notification),
                key=notification["properties"]["data_id"],
                callback=delivery_report
            )

        if len(error_messages)>0:
            logging.info("publishing %s error messages to %s", len(error_messages), kafka_error_topic )
            for error_message in error_messages:
                producer.produce(
                    topic=kafka_error_topic,
                    value=json.dumps(error_message),
                    callback=delivery_report
                )
        


    else:
        logging.debug("No new messages")


logging.info("closing consumer")
consumer.close()
