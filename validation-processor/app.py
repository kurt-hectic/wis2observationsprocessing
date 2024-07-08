import os
import json
import logging

from baseprocessor import BaseProcessor

from jsonschema import Draft202012Validator
from prometheus_client import  Counter


log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )


class ValidationProcessor(BaseProcessor):

    draft_202012_validator = None

    NR_INVALID_MESSAGES = Counter('invalid_messages_total', 'Number of messages with invalid notification schema')
    

    def __init__(self):
        BaseProcessor.__init__(self,group_id="my-consumer-deduplication-1")

        schema = json.loads(open("wis2-notification-message-bundled.json").read())
        Draft202012Validator.check_schema(schema)
        self.draft_202012_validator = Draft202012Validator(schema)
        


    def __process_messages__(self,notifications):
        error_messages = []
        
        initial_length = len(notifications)
        logging.debug(f"{initial_length} new messages")

        # only accept valid notificatons 
        notifications = [n for n in notifications if self.draft_202012_validator.is_valid(n)]
        nr_non_valid = initial_length - len(notifications)
        if nr_non_valid>0:
            logging.warning("filtered out %s non-valid records inside one batch ", nr_non_valid )
            self.NR_INVALID_MESSAGES.inc(nr_non_valid)
            # add non-valid messages to error list
            for n in [n for n in notifications if not self.draft_202012_validator.is_valid(n)]:
                error_messages.append({"reason" : "non valid" , "data" : n })

    
        keys = [n["properties"]["data_id"] for n in notifications]

        return notifications,keys,error_messages


if __name__ == "__main__":
   
    logging.info("starting validation processor")
    processor = ValidationProcessor()
    processor.start_consuming()