import json
import hashlib
import base64
import requests
import time
import random
import copy
import datetime
import os
import signal

import threading
import queue


notification_template = {
	"id": "5c997bac-c01d-4223-aac0-3b0f756d42c6",
	"type": "Feature",
	"version": "v04",
	"geometry": {
		"type": "Point",
		"coordinates": [
			80.4038,
			73.5082,
			46.4
		]
	},
	"properties": {
		"data_id": "ru-aviamettelecom/data/core/weather/surface-based-observations/synop/WIGOS_0-20000-0-20674_20240618T120000",
		"datetime": "2024-06-18T12:00:00Z",
		"pubtime": "2024-06-18T12:58:02Z",
		"integrity": {
			"method": "sha512",
			"value": "Z0DAfLv6xXpn62M3m3iyB0vamCIDMaLH8vIjSOIpoyCiqclOzbgphUbScSy6xJu3JotHpgjuezq7CNjdfJk9tg=="
		},
		"content": {
			"encoding": "base64",
			"value": "QlVGUgABUAQAABYAAB4AAAAAAAIAHAAH6AYSDAAAAAALAAABwMGWx1AAASMAABOIAAAAAMjA2NzQAAAAAAAAAAAAAAACgFRAJ6mqKSerAAAAAAAAAAAAAAAAAAAAgP0AMAkAwAAB8vzIAxqweABFwACKIBOQgTrYD3AQB/8D//A//+AAZADWGgNWiB/AAMgAiYAAZAH/+B//4GQAcBgAqgTgLgeAAIH4Hgfgf/AAAPwP+B+B/wPwP+B+B//4H//A/A//8D//gfA//8D//wCgA/0AgAQD/4H/wH6AP/gAGQA/oAAegP/gf/4ABkAP6AQAANYuA/oBAAB//4AH0AQAIB/YAUAAyAfAf2A/4AQQDTAH/ACMA//8D/8DwP/A//A//+B//8D//gf//4H//+B///gf/gf//A//+B//wP//8D///A///wP/wP/wPwA3Nzc3",
			"size": 336
		},
		"wigos_station_identifier": "0-20000-0-20674"
	},
	"links": [
		{
			"rel": "canonical",
			"type": "application/x-bufr",
			"href": "http://test-cache/data/cache/a/ru-aviamettelecom/data/core/weather/surface-based-observations/synop/WIGOS_0-20000-0-20674_20240618T120000",
			"length": 336
		}
	]
}


content_integrated_rate = float(os.environ.get("CONTENT_INTEGRATED_RATE"))  # % of messages should have the content included
integrity_checksum_rate = float(os.environ.get("INTEGRITY_CHECKSUM_RATE")) # % of messates should have wrong checksum
integrity_length_content_rate = float(os.environ.get("INTEGRITY_LENGTH_CONTENT_RATE")) # % of messages should have wrong lenght in content
integrity_length_link_rate = float(os.environ.get("INTEGRITY_LENGTH_LINK_RATE")) # % of messages should have wrong lenght in content
integrity_schema_rate = float(os.environ.get("INTEGRITY_SCHEMA_RATE")) # % of messages with incorrect schema
integrity_pubdate_rate = float(os.environ.get("INTEGRITY_PUBDATE_RATE")) # % of messages with non ISO pubdate
nr_duplicates_max = int(os.environ.get("NR_DUPLICATES_MAX"))
nr_duplicates_min = int(os.environ.get("NR_DUPLICATES_MIN"))
duplicate_max_delay_ms = int(os.environ.get("DUPLICATE_MAX_DELAY_MS"))
nr_caches = int(os.environ.get("NR_CACHES"))
msg_rate = int(os.environ.get("MSG_RATE"))

q = queue.Queue()

DONE = False
def shutdown_gracefully(self,signum, stackframe):
        print("exiting")
        global DONE
        DONE = True

def worker():
    while True:
        item = q.get()

        if item["pubtime"] <= datetime.datetime.now():
            print( json.dumps(item["notification"],indent=None))
        else:
            q.put(item)

        q.task_done()

threading.Thread(target=worker, daemon=True).start()

signal.signal(signal.SIGINT, shutdown_gracefully)
signal.signal(signal.SIGTERM, shutdown_gracefully)

counter = 0
while not DONE:

    counter = counter + 1
    
    n = copy.deepcopy(notification_template)

    n["properties"]["data_id"] = n["properties"]["data_id"] + "-" + str(counter)
    n["id"] = n["id"] + "-" + str(counter)

    if random.random() >= content_integrated_rate:
        n["properties"].pop("content")

    else:
        if random.random() < integrity_length_content_rate:
            n["properties"]["content"]["size"] = n["properties"]["content"]["size"] + 10
    
    if random.random() < integrity_checksum_rate:
        n["properties"]["integrity"]["value"] = n["properties"]["integrity"]["value"] + "xx"

    
    if random.random() < integrity_length_link_rate:
        n["links"][0]["length"] = n["links"][0]["length"] + 10

    if random.random() < integrity_schema_rate:
        n["findme"] = "I am not supposed to be here"

    if random.random() < integrity_pubdate_rate:
        n["properties"]["pubtime"] = "the day before yesterday"

    for i in range(1,random.randint(nr_duplicates_min,nr_duplicates_max)+1):
        n_new = copy.deepcopy(n)

        n_new["id"] = n_new["id"] + "-" + str(i)

        cache = "http://test-cache-{}".format(random.randint(1,nr_caches))

        n_new["links"][0]["href"] = n_new["links"][0]["href"].replace("http://test-cache",cache)
    

        payload = {
            "notification" : n_new , 
            "pubtime" : datetime.datetime.now() + datetime.timedelta(milliseconds= random.random() * duplicate_max_delay_ms * (0 if i==1 else 1))
        }

        q.put( payload )
        
        time.sleep(1 / msg_rate )


q.join()
print('All work completed')