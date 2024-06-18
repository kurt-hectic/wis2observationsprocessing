-- Description: ETL script for the Kafka topic "notifications-tostorage"
-- We store the current Kafka offset in a PostgreSQL table
INSERT INTO postgresql.public."kafka_table_offsets"
SELECT
  "_partition_id", 
  max("_partition_offset"), 
  CURRENT_TIMESTAMP,
  'upper_bound'
FROM kafka.default."notifications-tostorage" 
GROUP BY 
  "_partition_id", 
  CURRENT_TIMESTAMP
; 

-- Fetch 
INSERT INTO  
  minio.datalake.observations 
SELECT 
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
  "notification_wigos_id",
  "meta_broker",
  "meta_topic",
  "meta_time_received"
FROM
  kafka.default."notifications-tostorage" as ktable 
JOIN 
  postgresql.public."kafka_table_offsets" as offset_table_lower
ON ( 
    ktable."_partition_id" = offset_table_lower.kafka_partition_id
  AND 
    ktable."_partition_offset" > offset_table_lower.kafka_partition_offset
  AND
    offset_table_lower.bound = 'lower_bound'
)
JOIN
  postgresql.public."kafka_table_offsets" as offset_table_upper  
ON (
    ktable."_partition_id" = offset_table_upper.kafka_partition_id
  AND 
    ktable."_partition_offset" <= offset_table_upper.kafka_partition_offset
  AND
    offset_table_upper.bound = 'upper_bound'
);

DELETE FROM postgresql.public."kafka_table_offsets" WHERE bound = 'lower_bound';

UPDATE postgresql.public."kafka_table_offsets" SET bound = 'lower_bound' WHERE bound = 'upper_bound';
 
