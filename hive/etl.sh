#!/bin/sh
cat /opt/hive-etl/etl.sql |  beeline -u 'jdbc:hive2://localhost:10000/' --maxHistoryRows=0