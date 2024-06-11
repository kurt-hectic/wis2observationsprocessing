#!/bin/bash

while [ 0 ] ; do
	ls notifications/ |sort -R |tail -1 |while read file; do
		file="notifications/$file"
		mosquitto_pub -u everyone -P everyone --host mosquitto --port 1883 -t cache/a/wis2/us-noaa-synoptic/data/core/weather/surface-based-observations/synop -f $file
		#echo published $file
	done
	sleep $SLEEPRATE
done

