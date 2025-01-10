import json
from json_converter.json_mapper import JsonMapper


data = json.load(open("test_data/output-data.json"))

specification =  {
        'person.name': ['person_name'],
        'person.age': ['person_age']
}




print( json.dumps(JsonMapper(dict(specification)).map(data), indent=2) )