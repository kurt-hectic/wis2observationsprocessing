import os
import json
import logging
import jq

from baseprocessor import BaseProcessor

from jsonschema import Draft202012Validator
from prometheus_client import  Counter
from dateutil import parser as isoparser

log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )

jq_not_pubtime = jq.compile('.properties.pubtime')
jq_not_datetime = jq.compile('.properties.datetime')


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
        try:
            for d_exp in [jq_not_pubtime, jq_not_datetime]:
                v = d_exp.input(notification).first()
                if v:
                    isoparser.isoparse(v)
            
            return True
        
        except Exception as e: 
            logging.info(f"error parsing date '{v}' in notification {notification['id']}. Error: {e}")   
            return False
        


    def __process_messages__(self,notifications):
        error_messages = []
        
        initial_length = len(notifications)
        logging.debug(f"{initial_length} new messages")

        # only accept valid notificatons 
        notifications_new = [] 
        for n in notifications:
            (notifications_new if self.draft_202012_validator.is_valid(n) else error_messages).append(n)

        if len(error_messages)>0:
            logging.warning("filtered out %s non-valid records inside one batch ", len(error_messages) )
            self.NR_INVALID_MESSAGES.inc(len(error_messages))
            # add non-valid messages to error list
            for n in error_messages:
                error_messages.append({"reason" : "non valid" , "data" : n })

        notifications = notifications_new
        notifications_new = []
        error_messages_date = []

        # check dates
        for n in notifications:
            (notifications_new if self.__check_dates(n) else error_messages_date).append(n)

        if len(error_messages_date)>0:
            logging.warning("filtered out %s non-valid-date records inside one batch ", len(error_messages_date) )
            self.NR_INVALID_DATE_MESSAGES.inc(len(error_messages_date))
            # add non-valid messages to error list
            for n in error_messages_date:
                error_messages.append({"reason" : "date non valid" , "data" : n })

        notifications = notifications_new

        keys = [n["properties"]["data_id"] for n in notifications]

        return notifications,keys,error_messages


if __name__ == "__main__":
   
    logging.info("starting validation processor")
    processor = ValidationProcessor()
    processor.start_consuming()