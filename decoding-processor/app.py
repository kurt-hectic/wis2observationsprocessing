import os
import base64
import logging
import warnings

from baseprocessor import BaseProcessor

from jsonschema import Draft202012Validator
from prometheus_client import  Counter

with warnings.catch_warnings():
    warnings.filterwarnings("ignore",message=r"ecCodes .* or higher is recommended")
    from bufr2geojson import __version__, transform as as_geojson

log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )


NR_DECODING_ERRORS = Counter('decoding_errors_total', 'Number of decoding errors')


class DecodingProcessor(BaseProcessor):
     
    def __init__(self):
        BaseProcessor.__init__(self,group_id="my-consumer-decoding-1")


    def __process_messages__(self,notifications):

        initial_length = len(notifications)
        logging.debug(f"{initial_length} new messages")

        observations = []

        for notification in notifications:
            try:
                collections = self.decode_bufr(notification)

                for collection in collections:
                    observations.append({
                        "notification" : notification,
                        "data" : collection
                    })

            except Exception as e:
                logging.error("could not decode BUFR",exc_info=True)

        error_messages = []

        keys = [o["notification"]["properties"]["data_id"]+"-"+str(i) for i,o in enumerate(observations)]

        return observations,keys,error_messages
   

    @NR_DECODING_ERRORS.count_exceptions()
    def decode_bufr(self,notification):
        ret = []
        
        geojson = as_geojson(base64.b64decode(notification["properties"]["content"]["value"]))
            
        if geojson and not geojson == None:
            for collection in geojson:
                for key, item in collection.items():
                    ret.append( item['geojson'] )

        return ret
         

 


if __name__ == "__main__":
    logging.info("starting decoding processor")
    processor = DecodingProcessor()
    processor.start_consuming()