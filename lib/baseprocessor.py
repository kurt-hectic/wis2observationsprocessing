import time
import os
import json
import logging
import signal

from abc import ABC, abstractmethod
from confluent_kafka import Producer, Consumer
from prometheus_client import start_http_server, disable_created_metrics ,Counter

log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )


poll_timeout_seconds = int(os.getenv("POLL_TIMEOUT_SEC"))
poll_batch_size = int(os.getenv("POLL_BATCH_SIZE"))

kafka_broker = os.getenv("KAFKA_BROKER")
kafka_topic_name = os.getenv("KAFKA_TOPIC")
kafka_pubtopic_name = os.getenv("KAFKA_PUBTOPIC")
kafka_error_topic = os.getenv("KAFKA_ERROR_TOPIC")
group_id_env = os.getenv("GROUP_ID",None)

NR_PROCESSING_ERRORS = Counter('processing_errors_total', 'Number of processing errors')
NR_PROCESSED_MESSAGES = Counter('processed_messages_total', 'Number of processed messages')
NR_PUBLISHED_MESSAGES = Counter('published_messages_total', 'Number of messages published to pubtopic')
NR_KAFKA_PUB_ERRORS = Counter('kafka_publish_errors_total', 'Number of kafka publishing errors')

class BaseProcessor(ABC):

    consumer = None
    producer = None
    t = None
    
    DONE = False




    def __init__(self,group_id=None) -> None:
        super().__init__()

        signal.signal(signal.SIGINT, self.shutdown_gracefully)
        signal.signal(signal.SIGTERM, self.shutdown_gracefully)

        if group_id_env is not None:
            group_id = group_id_env
            logging.info(f"overriding group_id {group_id} with env var {group_id_env}")

        if not group_id:
            raise ValueError("need to set group_id")

        self.consumer = Consumer({'bootstrap.servers': kafka_broker,
            'group.id': group_id ,
            'enable.auto.commit' : True,
            'auto.offset.reset': 'latest'})
        logging.info(f"subscribing to {kafka_topic_name} on {kafka_broker} with group id {group_id}")
        self.consumer.subscribe([kafka_topic_name])

        if kafka_pubtopic_name:
            self.producer = Producer({'bootstrap.servers': kafka_broker})
            logging.info("created producer. Publishing to %s on %s", kafka_pubtopic_name, kafka_broker)

        disable_created_metrics()
        self.t = start_http_server(int(os.getenv("METRIC_PORT", "8000")))

    def start_consuming(self):
        
        while not self.DONE:
            messages = self.consumer.consume(num_messages=poll_batch_size, timeout=poll_timeout_seconds)
    
            if len(messages)>0:                
                messages = [message for message in messages if not message.error() ]
                notifications = [ json.loads( message.value()) for message in messages ]

                ok_messages,keys,error_messages = self.__process_messages__(notifications)

                for i,notification in enumerate(ok_messages):
                    self.producer.produce(
                        topic=kafka_pubtopic_name,
                        value=json.dumps(notification),
                        key=keys[i],
                        callback=self.delivery_report
                    )
                    self.producer.poll(0)
                    NR_PUBLISHED_MESSAGES.inc()
                if len(ok_messages)>0:
                    logging.info("published %s messages to %s", len(ok_messages), kafka_pubtopic_name )


                if len(error_messages)>0:
                    logging.info("publishing %s error messages to %s", len(error_messages), kafka_error_topic )
                    for error_message in error_messages:
                        self.producer.produce(
                            topic=kafka_error_topic,
                            value=json.dumps(error_message),
                            callback=self.delivery_report
                        )
                        self.producer.poll(0)
                        NR_PROCESSING_ERRORS.inc()

                if len(messages)>0:
                    self.consumer.commit(messages[-1],asynchronous=False) # commit the last message, 

                NR_PROCESSED_MESSAGES.inc(len(messages)) # count all messages, even if they had errors

            else:
                logging.debug("No new messages")


        logging.info("closing consumer")
        self.consumer.close()

    @abstractmethod
    def __process_messages__(self):  
        pass

    def delivery_report(self,err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            NR_KAFKA_PUB_ERRORS.inc()
            logging.error('Message delivery failed: {}'.format(err))
        else:
            logging.debug('Message delivered to %s [%s]', msg.topic() , msg.partition())

    def shutdown_gracefully(self,signum, stackframe):
        logging.info("shutdown initiated")
        global DONE
        DONE = True