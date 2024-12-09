import os
import json
import logging
#import jq

from baseprocessor import BaseProcessor

from jsonschema import Draft202012Validator
from prometheus_client import  Counter
from dateutil import parser as isoparser

log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )

#jq_not_pubtime = jq.compile('.properties.pubtime')
#jq_not_datetime = jq.compile('.properties.datetime')


class ValidationProcessor(BaseProcessor):

    draft_202012_validator = None

    NR_INVALID_MESSAGES = Counter('invalid_messages_total', 'Number of messages with invalid notification schema')
    NR_INVALID_DATE_MESSAGES = Counter('invalid_date_messages_total', 'Number of messages with invalid date')

    def __init__(self):
        BaseProcessor.__init__(self,group_id="my-consumer-deduplication-1")

        schema = json.loads(open("wis2-notification-message-bundled.json").read())
        Draft202012Validator.check_schema(schema)
        self.draft_202012_validator = Draft202012Validator(schema)

    
    def __check_dates(self,notification):
            
        for date_str in [notification["properties"]["pubtime"], notification["properties"]["datetime"]]:
            try:
                isoparser.isoparse(date_str)
            except Exception as e: 
                logging.info(f"error parsing date \"{date_str}\" in notification {notification['id']}. Error: {e}")   
                return False

        return True


    def __process_messages__(self,notifications):
        error_messages = []
        
        initial_length = len(notifications)
        logging.debug(f"{initial_length} new messages")

        # only accept valid notificatons 
        notifications_valid = []
        notifications_invalid = [] 
        for n in notifications:
            if self.draft_202012_validator.is_valid(n):
                notifications_valid.append(n)
            else:
                notifications_invalid.append(n)

        if len(notifications_invalid)>0:
            logging.warning("filtered out %s non-valid records inside one batch ", len(notifications_invalid) )
            self.NR_INVALID_MESSAGES.inc(len(notifications_invalid))
            # add non-valid messages to error list
            for n in notifications_invalid:
                error_messages.append({"reason" : "non valid schema" , "data" : n })


        notifications_valid_dates = []
        notifications_invalid_dates = []
        
        # check dates
        for n in notifications_valid:
            if self.__check_dates(n):
                notifications_valid_dates.append(n)
            else:
                notifications_invalid_dates.append(n)

        if len(notifications_invalid_dates)>0:
            logging.warning("filtered out %s non-valid-date records inside one batch ", len(notifications_invalid_dates) )
            self.NR_INVALID_DATE_MESSAGES.inc(len(notifications_invalid_dates))
            # add non-valid messages to error list
            for n in notifications_invalid_dates:
                error_messages.append({"reason" : "non valid date" , "data" : n })

        keys = [n["properties"]["data_id"] for n in notifications_valid_dates]

        return notifications_valid_dates, keys, error_messages


if __name__ == "__main__":
   
    logging.info("starting validation processor")
    processor = ValidationProcessor()
    processor.start_consuming()