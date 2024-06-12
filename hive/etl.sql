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
  