FROM
  kafka.default."notifications-tostorage" ktable 
JOIN 
  postgresql.public."kafka_table_offsets" offset_table
ON ( 
    ktable._partitiokafka_partition_id = offset_table.kafka_partition_id
  AND 
    ktable.kafka_table_offset > offset_table.max_kafka_table_offset) 
INSERT INTO TABLE 
  minio.datalake.observations 
SELECT 
  "kafka_partition_id", 
  "kafka_table_offset", 
  "kafka_timestamp",
  "wigos_id",
  "result_time",
  "phenomenon_time",
  "latitude",
  "longitude",
  "altitude",
  "observed_property",
  "observed_value",
  "observed_unit",
  "notification_data_id",
  "notification_pubtime",
  "notification_datetime",
  "notification_wigos_id",
  "meta_broker",
  "meta_topic",
  "meta_time_received"
  
INSERT OVERWRITE TABLE 
  postgresql.public. 
SELECT
  `kafka_partition_id`, 
  max(`kafka_table_offset`), 
  CURRENT_TIMESTAMP 
GROUP BY 
  `kafka_partition_id`, 
  CURRENT_TIMESTAMP;
  