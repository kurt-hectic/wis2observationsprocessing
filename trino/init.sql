CREATE TABLE IF NOT EXISTS postgresql.public."kafka_table_offsets" AS (
    SELECT "_partition_id" as "kafka_partition_id", 
    MIN("_partition_offset") - 1 as "kafka_partition_offset", 
    CURRENT_TIMESTAMP as "insert_time",
    'lower_bound' as "bound"
    FROM  kafka.default."notifications-tostorage" 
    GROUP BY 
    "_partition_id", 
    CURRENT_TIMESTAMP 
);

--CREATE SCHEMA minio.datalake
--WITH (location = 's3a://datalake/');
CREATE SCHEMA minio.datalake
WITH (location = 's3a://wis2obsprocessing/');

CREATE TABLE IF NOT EXISTS minio.datalake.observations 
( 
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
    format = 'ORC'
);