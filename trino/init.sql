CREATE TABLE IF NOT EXISTS postgresql.public."kafka_table_offsets" AS (
    SELECT
    "_partition_id" as "kafka_partition_id", 
    MIN("_partition_offset") - 1 as "max_kafka_table_offset", 
    CURRENT_TIMESTAMP as "insert_time"
    FROM 
    kafka.default."notifications-tostorage" 
    GROUP BY 
    "_partition_id", 
    CURRENT_TIMESTAMP 
);

CREATE SCHEMA minio.datalake
WITH (location = 's3a://datalake/');

CREATE TABLE IF NOT EXISTS minio.datalake.observations 
( 
    "kafka_partition_id" int,
    "kafka_table_offset" bigint,
    "kafka_timestamp" timestamp,
    "wigos_id" varchar,
    "result_time" timestamp,
    "phenomenon_time" varchar,
    "latitude" double,
    "longitude" double,
    "altitude" double,
    "observed_property" varchar,
    "observed_value" double,
    "observed_unit" varchar,
    "notification_data_id" varchar,
    "notification_pubtime" timestamp,
    "notification_wigos_id" varchar,
    "meta_broker"   varchar,
    "meta_topic"    varchar,
    "meta_time_received" timestamp
)
WITH (
    format = 'ORC',
    external_location = 's3a://datalake/observations'
);