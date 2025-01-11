import json
from dateutil import parser as isoparser
import pyjq


data = json.load(open("test_data/output-data.json"))

def list_select(*args):
    return args[0][args[1]]

def is_not_empty(*args):
    return args[0] is not None and not args[0] == ""

def format_iso(*args):
        datestr = args[0]
        if datestr is None or datestr == "":
            return None
        try:
            return isoparser.isoparse(datestr).replace(microsecond=0).isoformat()
        except Exception as e:
            return None

# specification =  {
#         'wigos_id': ['notification.properties.wigos_station_identifier'],
#         "result_time" : ['data.properties.resultTime', format_iso], 
#         "phenomenon_time" : ['notification.properties.phenomenonTime'],
#         "latitude": ['data.geometry.coordinates', list_select, 0],
#         "longitude": ['data.geometry.coordinates', list_select, 1],
#         "altitude": ['data.geometry.coordinates', list_select, 2],
#         "observed_property": ['data.properties.name'],
#         "observed_value": ['data.properties.value'],
#         "observed_unit": ['data.properties.units'],
#         "notification_data_id": ['notification.properties.data_id'],
#         "notification_pubtime": ['notification.properties.pubtime', format_iso],
#         "notification_datetime": ['notification.properties.datetime'],
#         "notification_wigos_id": ['notification.properties.wigos_station_identifier'],
#         "meta_broker": ['notification._meta.broker'],
#         "meta_topic": ['notification._meta.topic'],
#         "meta_time_received": ['notification._meta.time_received',format_iso],
#         "content_inlcuded" : ['notification.properties.content.value', is_not_empty ],
#         "test": ['notification.properties.testxx', default_to, False] 
# }


specification = """{
    wigos_id: .notification.properties.wigos_station_identifier, 
    result_time: .data.properties.resultTime, 
    phenomenon_time: .notification.properties.phenomenonTime, 
    latitude: .data.geometry.coordinates[0], 
    longitude: .data.geometry.coordinates[1], 
    altitude: .data.geometry.coordinates[2], 
    observed_property: .data.properties.name, 
    observed_value: .data.properties.value, 
    observed_unit: .data.properties.units, 
    notification_data_id: .notification.properties.data_id, 
    notification_pubtime: .notification.properties.pubtime, 
    notification_datetime: .notification.properties.datetime, 
    notification_wigos_id: .notification.properties.wigos_station_identifier, 
    meta_broker: .notification._meta.broker, 
    meta_topic: .notification._meta.topic, 
    meta_time_received: .notification._meta.time_received, 
    }"""


print(  json.dumps( pyjq.compile(specification).first( data ) , indent=2) )