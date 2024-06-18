import os
import logging
import hashlib
import base64
import urllib
import gzip
import requests
import random

from requests import session

from baseprocessor import BaseProcessor

from prometheus_client import  Counter, Summary


log_level = os.getenv("LOG_LEVEL", "INFO")
level = logging.getLevelName(log_level)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=level, 
    handlers=[  logging.StreamHandler()] )

ingegrity_methods =  [ "sha256", "sha384", "sha512", "sha3-256", "sha3-384", "sha3-512" ]

NR_INTEGRITY_ERRORS = Counter('integrity_errors_total', 'Number of integrity errors')
NR_CONTENT_ERRORS = Counter('content_fetching_errors_total', 'Number of content fetching errors')
DOWNLOAD_LATENCY = Summary('download_latency_seconds', 'Time spent downloading content')
CACHE_RELIABILITY = Summary('cache_reliability', 'Cache reliability')



def decode_content(content):

    encoding_method =  content.get("encoding","base64").lower()

    if encoding_method == "base64":
        content_value = base64.b64decode(content["value"])
    elif encoding_method == "utf8" or encoding_method == "utf-8":
        content_value = content["value"].encode("utf8")
    elif encoding_method == "gzip":
        content_value = gzip.decompress(content["value"])
    else:
        raise Exception(f"encoding method {encoding_method} not supported")
    
    return content_value

    

def integrity_check(notification):
    integrity_method =  notification["properties"]["integrity"]["method"].lower()

    if integrity_method not in ingegrity_methods:
        raise Exception(f"integrity method {integrity_method} not supported")

    content_bytes = decode_content(notification["properties"]["content"])
    
    if len(content_bytes) != notification["properties"]["content"]["size"]:
        raise Exception(f"content size mismatch for {notification['properties']['data_id']}")

    h = hashlib.new(integrity_method)
    h.update(content_bytes)
    checksum = base64.b64encode(h.digest()).decode("utf-8")

    original_checksum = notification["properties"]["integrity"]["value"] 

    if checksum != original_checksum:
        raise Exception(f"checksum mismatch for {notification['properties']['data_id']} ({checksum} vs {original_checksum})")
    
    return True

class ContentProcessor(BaseProcessor):
     
    session = None

    def __init__(self):
        BaseProcessor.__init__(self,group_id="my-consumer-content-1")

        self.session = requests.Session()
    
    def content_check(self,notification):
        if not "content" in notification["properties"]:
            try:
                content, download_time, cache = self.handle_content(notification)
            except Exception as e:
                logging.error(f"could not download content for {notification['properties']['data_id']} {e}")
                # TODO: process error in context of Kafka
                return False

            notification["properties"]["content"] = {
                "encoding": "base64",
                "value": base64.b64encode(content).decode("utf-8") ,
                "size": len(content)
            }

        return notification
    
    def handle_content(self,notification):
        
        # random shuffle to avoid always using the same cache
        for i,url in enumerate(sorted(notification["_meta"]["cache_links"],key=lambda x: random.random())):
            try:
                resp = self.session.get(url, timeout=10)
                resp.raise_for_status()

                logging.debug("downloaded {} in {}".format(url,resp.elapsed))
                download_time=resp.elapsed.total_seconds()*1000
                parsed_url = urllib.parse.urlparse(url)
                cache = parsed_url.netloc

                DOWNLOAD_LATENCY.observe(download_time)


                if i>0:
                    logging.info("downloaded data_id {} from cache link {} after trying {} other links".format(notification["properties"]["data_id"],url,i))

                CACHE_RELIABILITY.observe(i+1)
                return resp.content, download_time, cache

            except Exception as e:
                logging.info("could not download data_id {} from cache link {}. {}".format(notification["properties"]["data_id"],url,e))
                NR_CONTENT_ERRORS.inc()

        raise Exception(f"data not evailable from from any cache links " + ",".join(notification["_meta"]["cache_links"]) )
        # TODO: configure download process to use the chache as partition key?



    def __process_messages__(self,notifications):
   
        initial_length = len(notifications)
        logging.debug(f"{initial_length} new messages")

        # download content for each notification if not included
        notifications =  [ n for n in [ self.content_check(notification) for notification in notifications ] if n ]
        nr_with_content = len(notifications)
        logging.debug("number of notifications with content %s", nr_with_content)
        if initial_length-nr_with_content > 0:
            logging.error("removed %s because of no content",initial_length-nr_with_content)

        # validate content checksum and length
        error_messages = []
        for i,n in enumerate(notifications):
            try:
                integrity_check(n)
            except Exception as e:
                error_messages.append({"reason" : "integrity error" , "detail" : str(e) , "data" : notifications.pop(i) })
                logging.error(f"integrity error for {n['properties']['data_id']} {e}")

        nr_with_ingegrity = len(notifications)
        logging.debug("number of notifications with correct integrity and length %s",nr_with_ingegrity)
        if len(error_messages)>0:
            NR_INTEGRITY_ERRORS.inc(len(error_messages))
            logging.error("removed %s because of integrity or length",len(error_messages))

        keys = [n["properties"]["data_id"] for n in notifications]

        return notifications,keys,error_messages


if __name__ == "__main__":
   
    logging.info("starting deduplication processor")
    processor = ContentProcessor()
    processor.start_consuming()