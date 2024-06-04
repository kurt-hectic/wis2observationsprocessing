import logging
import time
import sys
import threading
import time
import json
import ssl
import os
import signal
import random
import string
import queue

import paho.mqtt.client as mqtt_paho

from confluent_kafka import Producer
from prometheus_client import start_http_server, Counter

from datetime import datetime
from uuid import uuid4

log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )

def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

kafka_broker = os.getenv("KAFKA_BROKER")
topics = [t.strip() for t in os.getenv("TOPICS","cache/a/wis2/#").split(",")]
wis_broker_host = os.getenv("WIS_BROKER_HOST")
wis_broker_port = int(os.getenv("WIS_BROKER_PORT"))
wis_user = os.getenv("WIS_USERNAME")
wis_pw = os.getenv("WIS_PASSWORD")
client_id = os.getenv("CLIENT_ID") + get_random_string(6)
#aws_broker = os.getenv("AWS_BROKER")
validate_ssl_cert = os.getenv("VALIDATE_SSL", "False").lower() in ["true","1","yes"]

# update stats every x notifications
threshold = int(os.getenv("REPORTING_THRESHOLD","100"))
# group batch_size records together before sending to Kinesis stream
batch_size = int(os.getenv("BATCH_SIZE","10"))
heartbeat_threshold = int(os.getenv("HEARTBEAT_THRESHOLD","300")) # send an update every 5 minutes


# Prometheus metrics and server
NR_EMPTY_MESSAGES = Counter('nr_emptymessages_total', 'Number of empty messages')    
NR_MESSAGES_WITHOUT_DATAID = Counter('nr_messageswithoutdataid_total', 'Number of messages without data_id')
NR_INVALID_JSON = Counter('nr_invalidjson_total', 'Number of messages with invalid JSON')
NR_KAFKA_PUB_ERRORS = Counter('kafka_publish_errors_total', 'Number of kafka publish errors')

t = start_http_server(int(os.getenv("METRIC_PORT", "8000")))

#logging.debug(f"from file {cert_text}")

kafka_topic_name = os.getenv("KAFKA_TOPIC")

q = queue.Queue()


def on_connect_wis(client, userdata, flags, reason_code, properties):
    """
    set the bad connection flag for rc >0, Sets onnected_flag if connected ok
    also subscribes to topics
    """
    logging.info("Connected flags: %s result code: %s ",flags,reason_code) 
    for topic in topics:   
        logging.info("subscribing to:"+str(topic))
        client_wis2.subscribe(topic,qos=1)

def on_subscribe(client,userdata,mid, reason_codes, properties):
    """removes mid values from subscribe list"""
    logging.info("in on subscribe callback result %s",mid)
    client.subscribe_flag=True

def on_message(client, userdata, msg):
    topic=msg.topic
    logging.debug("message received with topic %s", topic)
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    #logging.debug("message received")
    message_routing(client,topic,m_decode)
    
def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    logging.info("connection to broker has been lost")
    client.connected_flag=False
    client.disconnect_flag=True
            
def message_routing(client,topic,msg):
    #logging.debug("message_routing")
    logging.debug("routing topic: %s",topic)
    logging.debug("routing message: %s ",msg)
    
    # insert metadata into the message
    if not msg or msg.isspace():
        NR_EMPTY_MESSAGES.inc()
        logging.debug("discarding empty message published on %s", topic)
        return
    try:
        msg = json.loads(msg)
    except json.JSONDecodeError as e:
        NR_INVALID_JSON.inc()
        logging.error("cannot parse json message %s , %s , %s",msg,e,topic)
        return    

    # add uuid as data_id if not present    
    if not "properties" in msg or not "data_id" in msg["properties"]:
        if not "properties" in msg:
            msg["properties"] = {}
        msg["properties"] = { "data_id" : str(uuid4()) }
        NR_MESSAGES_WITHOUT_DATAID.inc()
        logging.warning("no data_id in message, adding %s",msg["properties"]["data_id"])

    msg["_meta"] = { "time_received" : datetime.now().isoformat() , "broker" : wis_broker_host , "topic" : topic }
    
    logging.debug("queuing topic %s with length %s and %s",topic,len(msg),msg)
    q.put( (topic,msg) )

        
def create_wis2_connection():
    transport = "websockets" if wis_broker_port==443 else "tcp"
    logging.info(f"creating wis2 connection to {wis_broker_host}:{wis_broker_port} using {wis_user}/{wis_pw} over {transport} with client id {client_id}, validate_ssl={validate_ssl_cert}")
    
    client = mqtt_paho.Client(mqtt_paho.CallbackAPIVersion.VERSION2, client_id=client_id, transport=transport,
         protocol=mqtt_paho.MQTTv311, clean_session=False)
                         
    client.username_pw_set(wis_user, wis_pw)
    if wis_broker_port != 1883:
        logging.info("setting up TLS connection")
        client.tls_set(certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED if validate_ssl_cert else ssl.CERT_NONE)
        client.tls_insecure_set(not validate_ssl_cert)
    
    client.on_message = on_message
    client.on_connect = on_connect_wis
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    
    client.connect(wis_broker_host,
                   port=wis_broker_port,
                   #clean_start=mqtt_paho.,
                   #properties=properties,
                   keepalive=60)
    
    return client
    

    
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        NR_KAFKA_PUB_ERRORS.inc()
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.debug('Message delivered to %s [%s]', msg.topic() , msg.partition())



class ConsumerThread(threading.Thread):

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(ConsumerThread,self).__init__()
        self.target = target
        self.name = name
        
        self.shutdown_flag = threading.Event()
        #self.kinesis = boto3.client('kinesis')

        #self.producer = KafkaProducer(bootstrap_servers="kafka:9092")
        self.producer = Producer({'bootstrap.servers': kafka_broker})

        logging.info("created Kafka connection")

        return


    def run(self):
        counter = 0
        #threshold = 100 
        before = datetime.now()
        #batch_size = 10

        while not self.shutdown_flag.is_set():
            if not q.empty():

                # batching items together
                records = []
                nr_failed = 0
                while not q.empty() and len(records)<batch_size :
                    records.append(  q.get() )

                
                for (topic,msg) in records: #TODO we may not need this batching, since the producer also has a queue
                    try:
                        # self.producer.send(
                        #     topic=kafka_topic_name,
                        #     value=json.dumps(msg).encode("utf-8"),
                        #     key=msg["properties"]["data_id"].encode("utf-8")
                        # )

                        self.producer.produce(
                            topic=kafka_topic_name,
                            value=json.dumps(msg),
                            key=msg["properties"]["data_id"],
                            on_delivery=delivery_report
                        )

                    except Exception as e:
                        logging.error(f"could not publish records to Kafka",exc_info=True)
                        q.put( (topic,msg) ) # put failed notifications back into the queue
                        nr_failed = nr_failed + 1
                        time.sleep(0.1)
                        continue

                    logging.debug("published %s records to Kafka",len(records))

                counter = counter + len(records) - nr_failed

                # processing of statistics
                if counter > threshold:
                    now = datetime.now()
                    d = int(threshold / (now - before).total_seconds()  )
                    logging.debug("receievd %s messages.. sending stats. Throughput %s per sec",threshold,d)

                    try:
                        data = [
                                        {
                                            'MetricName': 'notificationsReceivedFromBroker',
                                            'Dimensions': [
                                                {
                                                    'Name': 'Broker',
                                                    'Value': "obsdecoder_"+wis_broker_host
                                                },
                                            ],
                                            'Unit': 'Count',
                                            'Value':  counter
                                        },
                                    ]
                        # response = cloud_watch.put_metric_data(
                        #             MetricData = data,
                        #             Namespace = 'WIS2monitoring'
                        #         )
                    except Exception as a:
                        logging.error("could not publish stats %s ",e)

                    counter = 0
                    before = now
                
            else: 
                time.sleep(0.01)
        logging.info('Consumer Thread #%s stopped', self.ident)
        return


def service_shutdown(signum, frame):
    signame = signal.Signals(signum).name
    logging.info(f"received signal {signame}")

    try:
    
        client_wis2.loop_stop()
        client_wis2.disconnect()
        
        c.shutdown_flag.set()
        c.join()
    except Exception as e:
        logging.error("error shutting down, mqtt client likely not correctly initalized",exc_info=True)
        sys.exit(1)
            
if __name__ == '__main__':

    logging.info("starting up bridge service")

    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)
    
    try:
    
        c = ConsumerThread(name='consumer')
        c.start()
        
          
        logging.info("creating connection to WIS2")
        client_wis2 = create_wis2_connection()
        logging.info("connected to WIS2")

        client_wis2.loop_forever() #start loop
        logging.info("exiting main thread")
        
    except Exception as e:
        logging.error("error, exiting",exc_info=True)
        sys.exit(1)
