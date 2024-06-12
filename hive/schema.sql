CREATE EXTERNAL TABLE IF NOT EXISTS
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


CREATE TABLE IF NOT EXISTS
  kafka_table_offsets (
  `partition_id` INT,
  `max_offset` BIGINT,
  `insert_time` TIMESTAMP);

INSERT OVERWRITE TABLE 
  kafka_table_offsets 
SELECT
 `__partition`, 
 MIN(`__offset`) - 1, 
 CURRENT_TIMESTAMP 
FROM 
  kafka_table 
GROUP BY 
  `__partition`, 
  CURRENT_TIMESTAMP;

CREATE TABLE IF NOT EXISTS orc_observations_table  (
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
