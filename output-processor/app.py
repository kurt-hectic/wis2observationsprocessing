import os
import json
import logging
import jq
import signal
from datetime import datetime
from dateutil import parser as isoparser

from baseprocessor import BaseProcessor


from prometheus_client import Counter


log_level = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=log_level, 
    handlers=[  logging.StreamHandler()] )

NR_RECORDS_COMMITTED = Counter('records_committed_total', 'Number of records committed to the database')
NR_RECORDS_NOT_PARSED = Counter('records_not_parsed_total', 'Number of records not parsed')
NR_RECORD_EMPTY_VALUES = Counter('records_empty_values_total', 'Number of records with empty values')

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

# jq_observed_property = jq.compile('.notification.properties.name')
# jq_observed_value = jq.compile('.notification.properties.value')
# jq_observed_unit = jq.compile('.notification.properties.units')

jq_result_time = jq.compile('.data.properties.resultTime')
jq_phenomenon_time = jq.compile('.data.properties.phenomenonTime')


class OutputProcessor(BaseProcessor):

    conn = None

    def __init__(self):
        BaseProcessor.__init__(self,group_id="my-consumer-output-1")

    def __format_datetime(self,datestr):
        if datestr is None or datestr == "":
            return None
        try:
            return isoparser.isoparse(datestr).replace(microsecond=0).isoformat()
        except Exception as e:
            logging.error(f"error formatting date {datestr}. Error: {e}")
            return None


    def __process_messages__(self,observations):

    
        initial_length = len(observations)
        logging.debug(f"{initial_length} new messages")

        values = []
        keys = []
        for i,observation in enumerate(observations):
            
            try:

                logging.debug("processing observation %s", observation)

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
                     "result_time":self.__format_datetime(result_time),
                     "phenomenon_time": phenomenon_time,
                     "latitude":lat,
                     "longitude":lon,
                     "altitude":alt,
                     "observed_property":observed_property,
                     "observed_value":observed_value,
                     "observed_unit":observed_unit,
                     "notification_data_id":ndataid,
                     "notification_pubtime":self.__format_datetime(npubtime),
                     "notification_datetime":ndatetime,
                     "notification_wigos_id":nwigosid,
                     "meta_broker":meta_broker,
                     "meta_topic":meta_topic,
                     "meta_time_received":self.__format_datetime(meta_time_received)
                }

                key = f"{wigosid}-{ndataid}-{result_time}"

                if any( [v is None for v in d.values()] ):
                    NR_RECORD_EMPTY_VALUES.inc()
                    logging.warning("empty values in record %s", d)

                values.append(d)
                keys.append(key)

                logging.debug("processed observation %s", d)

            except Exception as e:
                NR_RECORDS_NOT_PARSED.inc()
                logging.error("error processing observation: %s. Error: %s", observation, e, exc_info=True)

        return values,keys,[]
    

if __name__ == "__main__":
   
    logging.info("starting output processor")
    processor = OutputProcessor()
    processor.start_consuming()