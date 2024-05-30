import os
import json
import logging
import jq
import psycopg2

from psycopg2.extras import execute_values
from confluent_kafka import  Consumer

log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )

poll_timeout_seconds = int(os.getenv("POLL_TIMEOUT_SEC"))
poll_batch_size = int(os.getenv("POLL_BATCH_SIZE"))

kafka_broker = os.getenv("KAFKA_BROKER")
kafka_topic_name = os.getenv("KAFKA_TOPIC")
kafka_pubtopic_name = os.getenv("KAFKA_PUBTOPIC")
kafka_broker = os.getenv("KAFKA_BROKER")

#consumer = KafkaConsumer(bootstrap_servers=kafka_broker, group_id='my-consumer-1')
consumer = Consumer({'bootstrap.servers': kafka_broker,
    'group.id': 'my-consumer-output-1',
    'auto.offset.reset': 'earliest'})
logging.info(f"subscribing to {kafka_topic_name}")
consumer.subscribe([kafka_topic_name])

# database connection
logging.info("establishing db connection")
dbname = os.getenv("DB_NAME")
dbuser = os.getenv("DB_USER")
dbpw = os.getenv("DB_PW")
dbhost = os.getenv("DB_HOST_NAME")
conn = psycopg2.connect(f"dbname={dbname} user={dbuser} password={dbpw} host={dbhost}")

#setup jq expressions

jq_geometry = jq.compile('.data.geometry.coordinates')
jq_wigosid = jq.compile('.data.properties.wigos_station_identifier')
jq_dataid = jq.compile('.notification.properties.data_id')
jq_meta_timereceived = jq.compile('.notification._meta.time_received')
jq_meta_topic = jq.compile('.notification._meta.topic')
jq_observed_property = jq.compile('.data.properties.name')


sql_insert = "INSERT INTO synopobservations (wigosid,latitude,longitude,elevation,observed_property,notification_data_id,meta_topic,meta_received_datetime) VALUES %s"

while True:
     
    #msg = consumer.poll(timeout_ms=poll_timeout_seconds*1000) # wait for messages 

    messages = consumer.consume(num_messages=poll_batch_size, timeout=poll_timeout_seconds)

    if len(messages)>0:
        
        # load notifications from all partitions
        #notifications = [ json.loads( n.value ) for partition in msg.keys() for n in msg[partition]]
        observations = [ json.loads( message.value()) for message in messages if not message.error() ]

        initial_length = len(observations)
        logging.debug(f"{initial_length} new messages")

        values = []
        for i,observation in enumerate(observations):

            wigosid = jq_wigosid.input(observation).first()
            dataid = jq_dataid.input(observation).first()
            (lat,lon,alt) = jq_geometry.input(observation).first()
            meta_topic = jq_meta_topic.input(observation).first()
            meta_time_received = jq_meta_timereceived.input(observation).first()
            observed_property = jq_observed_property.input(observation).first()


            logging.debug(f"lat: {lat}, lon: {lon}, alt: {alt}")

            tpl = (wigosid,lat,lon,alt,observed_property,dataid,meta_topic,meta_time_received)

            values.append(tpl)


        execute_values(conn.cursor(), sql_insert, values)
        logging.debug("added %s records to the database", len(values))


    

    else:
        logging.debug("No new messages")


