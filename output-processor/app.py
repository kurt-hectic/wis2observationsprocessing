import os
import json
import logging
import jq
import psycopg2
import signal

from psycopg2.extras import execute_values
from confluent_kafka import  Consumer
from prometheus_client import start_http_server, Counter, Summary


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

t = start_http_server(int(os.getenv("METRIC_PORT", "8000")))
NR_RECORDS_COMMITTED = Counter('records_committed_total', 'Number of records committed to the database')


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

jq_not_dataid = jq.compile('.notification.properties.data_id')
jq_not_pubtime = jq.compile('.notification.properties.pubtime')
jq_not_datetime = jq.compile('.notification.properties.datetime')
jq_not_wigosid = jq.compile('.notification.properties.wigos_station_identifier')

jq_meta_timereceived = jq.compile('.notification._meta.time_received')
jq_meta_topic = jq.compile('.notification._meta.topic')
jq_meta_broker = jq.compile('.notification._meta.broker')

jq_observed_property = jq.compile('.data.properties.name')
jq_observed_value = jq.compile('.data.properties.value')
jq_observed_unit = jq.compile('.data.properties.units')

jq_result_time = jq.compile('.data.properties.resultTime')
jq_phenomenon_time = jq.compile('.data.properties.phenomenonTime')


sql_insert = """INSERT INTO synopobservations 
    (wigosid,result_time,phenomenon_time,latitude,longitude,elevation,
    observed_property, observed_value, observed_unit,
    notification_data_id, notification_pubtime, notification_datetime, notification_wigosid,
    meta_broker,meta_topic,meta_received_datetime) VALUES %s"""

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
        
        # load notifications from all partitions
        #notifications = [ json.loads( n.value ) for partition in msg.keys() for n in msg[partition]]
        observations = [ json.loads( message.value()) for message in messages if not message.error() ]

        initial_length = len(observations)
        logging.debug(f"{initial_length} new messages")

        values = []
        for i,observation in enumerate(observations):

            wigosid = jq_wigosid.input(observation).first()
            result_time = jq_result_time.input(observation).first()
            phenomenon_time = jq_phenomenon_time.input(observation).first()
            (lat,lon,alt) = jq_geometry.input(observation).first()
            observed_property = jq_observed_property.input(observation).first()
            observed_value = jq_observed_value.input(observation).first()
            observed_unit = jq_observed_unit.input(observation).first()

            ndataid = jq_not_dataid.input(observation).first()
            npubtime = jq_not_pubtime.input(observation).first()
            ndatetime = jq_not_datetime.input(observation).first()
            nwigosid = jq_not_wigosid.input(observation).first()

            meta_topic = jq_meta_topic.input(observation).first()
            meta_time_received = jq_meta_timereceived.input(observation).first()
            meta_broker = jq_meta_broker.input(observation).first()

            tpl = (wigosid,result_time,phenomenon_time,lat,lon,alt,observed_property,observed_value,observed_unit,ndataid,npubtime,ndatetime,nwigosid,meta_broker,meta_topic,meta_time_received)

            values.append(tpl)


        execute_values(conn.cursor(), sql_insert, values)
        conn.commit()

        NR_RECORDS_COMMITTED.inc(len(values))    
        logging.debug("added %s records to the database", len(values))


    

    else:
        logging.debug("No new messages")


logging.info("closing consumer")
consumer.close()