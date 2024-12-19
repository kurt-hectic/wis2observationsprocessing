import os
import json
import logging
import jq

from baseprocessor import BaseProcessor

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
from prometheus_client import  Counter

log_level = os.getenv("LOG_LEVEL", "INFO")
remove_invalid_messages = os.getenv("REMOVE_INVALID_MESSAGES", "True").lower() in ["true","1","yes"]
remove_duplicate_messages = os.getenv("REMOVE_DUPLICATE_MESSAGES", "True").lower() in ["true","1","yes"]

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=log_level, 
    handlers=[  logging.StreamHandler()] )


jq_canonical_links = jq.compile('.links[] | select(.rel=="canonical").href')


class ValidationProcessor(BaseProcessor):

    draft_202012_validator = None

    NR_INVALID_MESSAGES = Counter('invalid_messages_total', 'Number of messages with invalid notification schema')
    NR_INVALID_DATE_MESSAGES = Counter('invalid_date_messages_total', 'Number of messages with invalid date')
    NR_DUPLICATES = Counter('duplicate_messages_total', 'Number of duplicate messages')
 

    def __init__(self):
        BaseProcessor.__init__(self,group_id="my-consumer-deduplication-1")

        schema = json.loads(open("wis2-notification-message-bundled.json").read())
        Draft202012Validator.check_schema(schema)
        self.draft_202012_validator = Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


    def __process_messages__(self,notifications):
        logging.debug(f"{len(notifications)} new messages")

        notifications_valid = []
        nr_invalid = 0
        for n in notifications:
            try:
                self.draft_202012_validator.validate(n)
                n["_meta"]["valid_schema"] = True
                notifications_valid.append(n)
            except ValidationError as e:
                logging.warning("Validation error: %s", e.message)
                n["_meta"]["valid_schema"] = False
                if not remove_invalid_messages:
                    notifications_valid.append(n)
                nr_invalid += 1


        if nr_invalid>0:
            logging.warning(f"{nr_invalid} non-valid records inside one batch. Removing {remove_invalid_messages}")
            self.NR_INVALID_MESSAGES.inc(nr_invalid)
    
        notifications = notifications_valid
        # check duplicates
        if remove_duplicate_messages:
            # filter out possible duplicates inside the batch
            data_ids_links = {}
            notifications_new = []
            initial_length = len(notifications)
            for n in notifications:
                if n["properties"]["data_id"] not in data_ids_links:
                    data_ids_links[n["properties"]["data_id"]] = []
                    notifications_new.append(n)
                data_ids_links[n["properties"]["data_id"]].append( jq_canonical_links.input(n).all() ) 

            notifications = []
            for n in notifications_new:
                n["_meta"]["cache_links"] = list(set([url for lou in data_ids_links[n["properties"]["data_id"]] for url in lou ])) #flatten list of list of links and only use unique links
                notifications.append(n)

            nr_duplicate_in_batch = (initial_length)-len(notifications)
            if nr_duplicate_in_batch>0:
                self.NR_DUPLICATES.inc(nr_duplicate_in_batch)
                logging.debug("filtered out %s duplicate records inside one batch ", nr_duplicate_in_batch )

        keys = [n["properties"]["data_id"] for n in notifications]

        return notifications, keys, []


if __name__ == "__main__":
   
    logging.info("starting validation processor")
    processor = ValidationProcessor()
    processor.start_consuming()