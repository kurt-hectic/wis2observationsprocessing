import json

from jsonschema import Draft202012Validator

schema = json.loads(open(r"validation-processor/wis2-notification-message-bundled.json").read())

test_notification = {
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
		"data_id": "ru-aviamettelecom/data/core/weather/surface-based-observations/synop/WIGOS_0-20000-0-20674_20240618T120000-1769",
		"datetime": "xxx2024-06-18T12:00:00Z",
		"pubtime": "xxx2024-06-18T12:58:02Z",
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
			"href": "http://test-cache-2/data/cache/a/ru-aviamettelecom/data/core/weather/surface-based-observations/synop/WIGOS_0-20000-0-20674_20240618T120000",
			"length": 336
		}
	],
	"_meta": {
		"time_received": "2024-12-18T08:17:11.120581",
		"broker": "mosquitto",
		"topic": "cache/a/wis2/ru-aviamettelecom/data/core/weather/surface-based-observations/synop"
	}
}

counter=1000000

test_notification["id"] = test_notification["id"][:-7] + "{:07d}".format(counter)

Draft202012Validator.check_schema(schema)
draft_202012_validator = Draft202012Validator(schema=schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


try:
    draft_202012_validator.validate(test_notification)
except Exception as e:
    print(e.message)
    print("Validation failed")