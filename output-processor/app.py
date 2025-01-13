import os
import logging
import pyjq
import uuid

from abc import ABC, abstractmethod

from baseprocessor import BaseProcessor


from prometheus_client import Counter


log_level = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=log_level, 
    handlers=[  logging.StreamHandler()] )

NR_RECORDS_COMMITTED = Counter('records_committed_total', 'Number of records committed to the database')
NR_RECORDS_NOT_PARSED = Counter('records_not_parsed_total', 'Number of records not parsed')
NR_RECORD_EMPTY_VALUES = Counter('records_empty_values_total', 'Number of records with empty values')



class OutputProcessor(BaseProcessor):

    conn = None

    def __init__(self,pattern='{.}',group_id=None,keys=[]):
        BaseProcessor.__init__(self,group_id=group_id)
        logging.info("starting output processor with pattern %s and keys %s and group %s", pattern, keys, group_id)
        self.pattern = pyjq.compile( pattern )
        self.keys = keys            

    def __process_messages__(self,observations):
        initial_length = len(observations)
        logging.debug(f"{initial_length} new messages")

        values = []
        keys = []
        for observation in observations:         
            try:
                logging.debug("processing observation %s", observation)

                d = self.pattern.first(observation)
                values.append(d)

                if not all( [k in d for k in self.keys] ):
                    logging.warning("keys %s not found in record %s",self.keys, d)
                    key = uuid.uuid4()
                else:
                    key = "-".join([str(d[k]) for k in self.keys])
                keys.append(key)
                
                if any( [v is None for v in d.values()] ):
                    NR_RECORD_EMPTY_VALUES.inc()
                    logging.warning("empty values in record %s", d)

                logging.debug("processed observation %s", d)

            except Exception as e:
                NR_RECORDS_NOT_PARSED.inc()
                logging.error("error processing observation: %s. Error: %s", observation, e, exc_info=True)

        return values,keys,[]
    

if __name__ == "__main__":

    pattern = open( os.getenv("DATA_MAPPING_FILE"),encoding="utf8",mode="r").read()  
    group_id = os.getenv("GROUP_ID")
    keys = os.getenv("KEYS").split(",")
    if len(keys) == 0:
        raise ValueError("keys must be defined")
   
    processor = OutputProcessor(pattern=pattern,group_id=group_id,keys=keys)
    processor.start_consuming()