import os
import json
import logging
import requests, urllib
import base64, gzip, zlib
import hashlib

from confluent_kafka import Producer, Consumer

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
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.debug('Message delivered to %s [%s]', msg.topic() , msg.partition())


def handle_content(notification):
        url = [ l["href"] for l in notification["links"] if l["rel"] == "canonical" ][0]
        resp = session.request("GET",url, timeout=4)
        resp.raise_for_status()
        logging.debug("downloaded {} in {}".format(url,resp.elapsed))
        download_time=resp.elapsed.total_seconds()*1000
        parsed_url = urllib.parse.urlparse(url)
        cache = parsed_url.netloc

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
        logging.warning(f"integrity method {integrity_method} not supported")
        return False

    content_bytes = decode_content(notification["properties"]["content"])

    if len(content_bytes) != notification["properties"]["content"]["size"]:
        logging.warning(f"content size mismatch for {notification['properties']['data_id']}")
        return False

    h = hashlib.new(integrity_method)
    h.update(content_bytes)
    checksum = base64.b64encode(h.digest()).decode("utf-8")

    original_checksum = notification["properties"]["integrity"]["value"] 

    if checksum != original_checksum:
        logging.warning(f"checksum mismatch for {notification['properties']['data_id']} ({checksum} vs {original_checksum})")
        return False
    
    return notification


while True:
     
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
        notifications =  [ n for n in [ integrity_check(notification) for notification in notifications ] if n ]
        nr_with_ingegrity = len(notifications)
        logging.debug("number of notifications with correct integrity and length %s",nr_with_ingegrity)
        if nr_with_content-nr_with_ingegrity>0:
            logging.warning("removed %s because of integrity or length",nr_with_content-nr_with_ingegrity)

        logging.debug("sending %s notifications to Kafka %s", len(notifications),kafka_pubtopic_name)
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


