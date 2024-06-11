DROP TABLE IF EXISTS kafka_table;

-- 'wigos_id': '0-840-0-KHLG', 'result_time': '2024-05-29T08:40:00Z', 'phenomenon_time': '2024-05-29T08:40:00Z', 'latitude': -80.64444, 'longitude': 40.17028, 'altitude': 363.9, 'observed_property': 'air_temperature', 'observed_value': 12.2, 'observed_unit': 'Celsius', 'notification_data_id': 'us-noaa-synoptic/data/core/weather/surface-based-observations/synop/WIGOS_0-840-0-KHLG_20240529T084000', 'notification_pubtime': '2024-05-29T08:45:07Z', 'notification_datetime': '2024-05-29T08:40:00Z', 'notification_wigos_id': '0-840-0-KHLG', 'meta_broker': 'mosquitto', 'meta_topic': 'cache/a/wis2/us-noaa-synoptic/data/core/weather/surface-based-observations/synop', 
-- 'meta_time_received': '2024-06-11T16:31:03.606181'}

CREATE EXTERNAL TABLE 
  kafka_table (
    `wigos_id` STRING,
    `result_time` TIMESTAMP,
    `phenomenon_time` STRING,
    `latitude` DOUBLE,
    `longitude` DOUBLE,
    `altitude` DOUBLE,
    `observed_property` STRING,
    `observed_value` DOUBLE,
    `observed_unit` STRING,
    `notification_data_id` STRING,
    `notification_pubtime` TIMESTAMP,
    `notification_datetime` TIMESTAMP,
    `notification_wigos_id` STRING,
    `meta_broker` STRING,
    `meta_topic` STRING,
    `meta_time_received` TIMESTAMP)
STORED BY 
  'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES ( 
  "kafka.topic" = "notifications-tohive",
  "kafka.bootstrap.servers" = "kafka:9092",
  "kafka.serde.class" = "org.apache.hadoop.hive.serde2.JsonSerDe",
  "timestamp.formats" = "yyyy-MM-dd'T'HH:mm:ss'Z',yyyy-MM-dd'T'HH:mm:ss,YYYY-MM-DD HH:MM:SS.SSS"
  );

  select wigos_id,result_time,phenomenon_time,notification_pubtime,meta_time_received from kafka_table limit 10;

  SELECT 
  COUNT(*)
FROM 
  kafka_table 
WHERE 
  `__timestamp` >  1000 * TO_UNIX_TIMESTAMP(CURRENT_TIMESTAMP - INTERVAL '10' MINUTES);

DROP TABLE 
  kafka_table_offsets;
  
CREATE TABLE 
  kafka_table_offsets (
  `partition_id` INT,
  `max_offset` BIGINT,
  `insert_time` TIMESTAMP);



  CREATE TABLE 
  orc_observations_table (
    `partition_id` INT,
    `koffset` BIGINT,
    `ktimestamp` BIGINT,
    `wigos_id` STRING,
    `result_time` TIMESTAMP,
    `phenomenon_time` STRING,
    `latitude` DOUBLE,
    `longitude` DOUBLE,
    `altitude` DOUBLE,
    `observed_property` STRING,
    `observed_value` DOUBLE,
    `observed_unit` STRING,
    `notification_data_id` STRING,
    `notification_pubtime` TIMESTAMP,
    `notification_datetime` TIMESTAMP,
    `notification_wigos_id` STRING,
    `meta_broker` STRING,
    `meta_topic` STRING,
    `meta_time_received` TIMESTAMP)
STORED AS ORC;




FROM
  kafka_table ktable 
JOIN 
  kafka_table_offsets offset_table
ON ( 
    ktable.`__partition` = offset_table.`partition_id`
  AND 
    ktable.`__offset` > offset_table.`max_offset`) 
INSERT INTO TABLE 
  orc_observations_table 
SELECT 
  `__partition`, 
  `__offset`, 
  `__timestamp`,
  `wigos_id`,
  `result_time`,
  `phenomenon_time`,
  `latitude`,
  `longitude`,
  `altitude`,
  `observed_property`,
  `observed_value`,
  `observed_unit`,
  `notification_data_id`,
  `notification_pubtime`,
  `notification_datetime`,
  `notification_wigos_id`,
  `meta_broker`,
  `meta_topic`,
  `meta_time_received`
  
INSERT OVERWRITE TABLE 
  kafka_table_offsets 
SELECT
  `__partition`, 
  max(`__offset`), 
  CURRENT_TIMESTAMP 
GROUP BY 
  `__partition`, 
  CURRENT_TIMESTAMP;