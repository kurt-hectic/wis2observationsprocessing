-- CREATE TABLE IF NOT EXISTS postgresql.public."kafka_table_offsets" AS (
--     SELECT "_partition_id" as "kafka_partition_id", 
--     MIN("_partition_offset") - 1 as "kafka_partition_offset", 
--     CURRENT_TIMESTAMP as "insert_time",
--     'lower_bound' as "bound"
--     FROM  kafka.default."notifications-tostorage" 
--     GROUP BY 
--     "_partition_id", 
--     CURRENT_TIMESTAMP 
-- );

CREATE TABLE IF NOT EXISTS postgresql.public."kafka_table_offsets" (
    "kafka_partition_id" integer,
    "kafka_partition_offset" bigint,
    "insert_time" timestamp,
    "bound" varchar,
    "type"  varchar
);

INSERT INTO postgresql.public."kafka_table_offsets" 
("kafka_partition_id", "kafka_partition_offset", "insert_time", "bound", "type")
SELECT * FROM (
VALUES 
(0, 0, CURRENT_TIMESTAMP, 'lower_bound', 'notifications'),
(0, 0, CURRENT_TIMESTAMP, 'lower_bound', 'observations')
) source_data 
WHERE NOT EXISTS(
    SELECT NULL
    FROM postgresql.public."kafka_table_offsets"
);

CREATE SCHEMA IF NOT EXISTS minio.datalake
WITH (location = 's3a://datalake/');
--CREATE SCHEMA IF NOT EXISTS minio.datalake
--WITH (location = 's3a://wis2obsprocessing/');

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
    "notification_pubtime" varchar,
    "notification_wigos_id" varchar,
    "meta_broker"   varchar,
    "meta_topic"    varchar,
    "meta_time_received" timestamp,
    "created" timestamp 
)
WITH (
    format = 'ORC'
);

CREATE TABLE IF NOT EXISTS minio.datalake.notifications 
( 
    "id" varchar,
    "data_id" varchar,
    "pubtime" varchar,
    "datetime" varchar,
    "integrity_method" varchar,
    "link" varchar,
    "meta_broker"  varchar,
    "meta_cache"   varchar,
    "meta_topic"   varchar,
    "meta_time_received" timestamp,
    "meta_time_download_ms" double,
    "meta_status_code" integer,
    "meta_content_status" varchar,
    "meta_schema_valid" boolean,
    "content_included" boolean,
    "created" timestamp 
)
WITH (
    format = 'ORC'
);