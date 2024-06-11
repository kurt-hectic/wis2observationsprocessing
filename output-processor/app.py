import os
import json
import logging
import jq
import psycopg2
import signal

from psycopg2.extras import execute_values
from baseprocessor import BaseProcessor


from psycopg2.extras import execute_values
from prometheus_client import Counter


log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )

NR_RECORDS_COMMITTED = Counter('records_committed_total', 'Number of records committed to the database')
NR_RECORDS_NOT_PARSED = Counter('records_not_parsed_total', 'Number of records not parsed')

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

class OutputProcessor(BaseProcessor):

    conn = None

    def __init__(self):
        BaseProcessor.__init__(self,group_id="my-consumer-output-1")

        # database connection
        # logging.info("establishing db connection")
        # dbname = os.getenv("DB_NAME")
        # dbuser = os.getenv("DB_USER")
        # dbpw = os.getenv("DB_PW")
        # dbhost = os.getenv("DB_HOST_NAME")
        # self.conn = psycopg2.connect(f"dbname={dbname} user={dbuser} password={dbpw} host={dbhost}")



    def __process_messages__(self,observations):

    
        initial_length = len(observations)
        logging.debug(f"{initial_length} new messages")

        values = []
        keys = []
        for i,observation in enumerate(observations):
            
            try:

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

                #tpl = (wigosid,result_time,phenomenon_time,lat,lon,alt,observed_property,observed_value,observed_unit,ndataid,npubtime,ndatetime,nwigosid,meta_broker,meta_topic,meta_time_received)

                d = { "wigos_id": wigosid,
                     "result_time":result_time,
                     "phenomenon_time": phenomenon_time,
                     "latitude":lat,
                     "longitude":lon,
                     "altitude":alt,
                     "observed_property":observed_property,
                     "observed_value":observed_value,
                     "observed_unit":observed_unit,
                     "notification_data_id":ndataid,
                     "notification_pubtime":npubtime,
                     "notification_datetime":ndatetime,
                     "notification_wigos_id":nwigosid,
                     "meta_broker":meta_broker,
                     "meta_topic":meta_topic,
                     "meta_time_received":meta_time_received}

                key = f"{wigosid}-{ndataid}-{result_time}"

                #values.append(tpl)
                values.append(d)
                keys.append(key)

                logging.debug(f"processed observation {d}")

            except Exception as e:
                NR_RECORDS_NOT_PARSED.inc()
                logging.error(f"error processing observation: {observation}. Error: {e}")

        #execute_values(self.conn.cursor(), sql_insert, values)
        #self.conn.commit()

        #NR_RECORDS_COMMITTED.inc(len(values))    
        #logging.debug("added %s records to the database", len(values))


        return values,keys,[]
    


if __name__ == "__main__":
   
    logging.info("starting output processor")
    processor = OutputProcessor()
    processor.start_consuming()