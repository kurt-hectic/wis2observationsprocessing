import os
import logging
import hashlib
import base64
import urllib
import gzip
import requests
import random
import jq
import threading
import math

from requests import session

from baseprocessor import BaseProcessor

from prometheus_client import  Counter, Summary


nr_threads = int(os.getenv("NR_THREADS", "1"))
log_level = os.getenv("LOG_LEVEL", "INFO")

remove_no_content = os.getenv("REMOVE_NO_CONTENT", "True").lower() in ["true","1","y","yes"] # if true, notifications with no content will be removed 
download_mode = os.getenv("DOWNLOAD_MODE", "True").lower() in ["true","1","y","yes"] # if false, content will not be downloaded (GET) but only its presence checked (HEAD). No removal of invalid content will be done 

jq_canonical_links = jq.compile('.links[] | select(.rel=="canonical").length')

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',level=log_level, 
    handlers=[  logging.StreamHandler()] )

ingegrity_methods =  [ "sha256", "sha384", "sha512", "sha3-256", "sha3-384", "sha3-512" ]

NR_INTEGRITY_ERRORS = Counter('content_integrity_errors_total', 'Number of integrity errors')
NR_CONTENT_ERRORS = Counter('content_fetching_errors_total', 'Number of content fetching errors')
NR_DOWNLOAD_ERRORS = Counter('content_download_errors_total', 'Number of download errors')
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
    
    con_size = notification["properties"]["content"]["size"]
    if len(content_bytes) != con_size:
        raise Exception(f"content size {len(content_bytes)} mismatch with size {con_size} provided in content element for {notification['properties']['data_id']}")
    
    can_size = jq_canonical_links.input(notification).first()
    if len(content_bytes) != can_size :
        raise Exception(f"content size {len(content_bytes)} mismatch with size {can_size} provided in link for {notification['properties']['data_id']}")

    h = hashlib.new(integrity_method)
    h.update(content_bytes)
    checksum = base64.b64encode(h.digest()).decode("utf-8")

    original_checksum = notification["properties"]["integrity"]["value"] 

    if checksum != original_checksum:
        raise Exception(f"checksum mismatch for {notification['properties']['data_id']} ({checksum} vs {original_checksum})")
    
    return True

def chunks(lst, nr_chunks):
    """Yield successive n chunks from lst."""
    chunk_size = math.ceil( len(lst) / nr_chunks )
    if chunk_size == 0:
        return []
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

class ContentProcessor(BaseProcessor):
     
    session = None

    def __init__(self):
        logging.info("initializing content processor download mode %s and remove no content %s",download_mode,remove_no_content)
        BaseProcessor.__init__(self,group_id="my-consumer-content-1")

        self.session = requests.Session()
    
    
    def handle_content(self,notification):
        
        # random shuffle to avoid always using the same cache
        for i,url in enumerate(sorted(notification["_meta"]["cache_links"],key=lambda x: random.random())):
            try:
                resp = self.session.get(url, timeout=10)
                resp.raise_for_status()

                logging.debug("downloaded {} in {}".format(url,resp.elapsed))
                if i>0:
                    logging.info("downloaded data_id {} from cache link {} after trying {} other links".format(notification["properties"]["data_id"],url,i))

                CACHE_RELIABILITY.observe(i+1)

                return resp

            except Exception as e:
                logging.warning("could not download data_id {} from cache link {}. {}".format(notification["properties"]["data_id"],url,e))
                NR_DOWNLOAD_ERRORS.inc()

        raise Exception(f"data not evailable from from any cache links " + ",".join(notification["_meta"]["cache_links"]) )
        # TODO: configure download process to use the chache as partition key?

    def __process_message_download__(self,notification):
        if not "content" in notification["properties"]:
            try:
                resp = self.handle_content(notification)

                notification["properties"]["content"] = {
                    "encoding": "base64",
                    "value": base64.b64encode(resp.content).decode("utf-8") ,
                    "size": len(resp.content)
                }

                notification["_meta"]["cache"] = urllib.parse.urlparse(resp.url).netloc
                notification["_meta"]["download_time"] = resp.elapsed.total_seconds()
                notification["_meta"]["status_code"] = resp.status_code
                notification["_meta"]["content_status"] = "downloaded" 
            except Exception as e:
                logging.error(f"could not download content for {notification['properties']['data_id']} {e}")
                notification["_meta"]["content_status"] = "download_error"
                NR_CONTENT_ERRORS.inc()
                
        else:
            notification["_meta"]["content_status"] = "embedded"

        
        if notification["_meta"]["content_status"] != "download_error":
            try:
                integrity_check(notification)
            except Exception as e:
                notification["_meta"]["content_status"] = "integrity_error"
                logging.error(f"integrity error for {notification['properties']['data_id']} {e}")
                NR_INTEGRITY_ERRORS.inc()

        return notification
    
    def __process_message_head(self,notification):
        
        # do not attempt to download content for origin messages
        if notification["_meta"]["topic"].startswith("origin"):
            return notification
        
        try:
            resp = self.session.head(notification["links"][0]["href"], timeout=10)

            notification["_meta"]["cache"] = urllib.parse.urlparse(resp.url).netloc
            notification["_meta"]["download_time"] = resp.elapsed.total_seconds()
            notification["_meta"]["status_code"] = resp.status_code

            resp.raise_for_status()

            can_size = jq_canonical_links.input(notification).first()
            notification["_meta"]["content_status"] = "checked" if can_size == int(resp.headers["Content-Length"]) else "size_error"
        except Exception as e:
            logging.error(f"could not check content for {notification['properties']['data_id']} {e}")
            notification["_meta"]["content_status"] = "download_error"
            NR_CONTENT_ERRORS.inc()
                
        return notification


    def __process_messages_thread__(self,notification_chunk,notifications):
        for notification in notification_chunk:
            
            if download_mode:
                notification = self.__process_message_download__(notification)
            else:
                notification = self.__process_message_head(notification)
            
            notifications.append(notification)


    def __process_messages__(self,notifications):
   
        initial_length = len(notifications)
        logging.debug(f"{initial_length} new messages")

        if len(notifications) > 0:
            jobs = []
            notifications_new = []
            for chunk in chunks(notifications,nr_threads):
                logging.debug("starting thread with %s notifications",len(chunk))
                jobs.append(threading.Thread(target=self.__process_messages_thread__(chunk,notifications_new)))

            for i, j in enumerate(jobs):
                logging.debug("starting thread %d", i)
                j.start()

            logging.debug("waiting for %d threads to finish", len(jobs))
            
            for i, j in enumerate(jobs):
                logging.debug("waiting for thread %d to finish", i)
                j.join()

            notifications = [ n for n in notifications_new if not n["_meta"]["content_status"].endswith("_error") or not remove_no_content ]

            nr_removed = initial_length - len(notifications)
            if nr_removed > 0:
                logging.warning("number of notifications removed due to content download issues %s", nr_removed)
    
            keys = [n["properties"]["data_id"] for n in notifications]

            return notifications,keys,[]
        else:
            return [],[],[]



if __name__ == "__main__":
   
    logging.info("starting content processor")
    processor = ContentProcessor()
    processor.start_consuming()