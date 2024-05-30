CREATE TABLE synopobservations (
  id SERIAL PRIMARY KEY,
  wigosid varchar(255) DEFAULT NULL,
  result_time timestamp  DEFAULT NULL,
  phenomenon_time varchar(255) DEFAULT NULL,
  latitude float DEFAULT NULL,
  longitude float DEFAULT NULL,
  elevation float DEFAULT NULL,
  observed_property varchar(255) DEFAULT NULL,
  observed_value float DEFAULT NULL,
  observed_unit varchar(10) DEFAULT NULL,

  notification_data_id varchar(255) DEFAULT NULL,
  notification_pubtime timestamp  DEFAULT NULL,
  notification_datetime timestamp  DEFAULT NULL,
  notification_wigosid varchar(255) DEFAULT NULL,
  
  meta_broker varchar(255) DEFAULT NULL,
  meta_topic varchar(255) DEFAULT NULL,
  meta_received_datetime timestamp  DEFAULT NULL,
  date_inserted timestamp NOT NULL DEFAULT NOW()
) ;

CREATE INDEX ON synopobservations (date_inserted);
CREATE INDEX ON synopobservations (meta_topic);
CREATE INDEX ON synopobservations (observed_property);
CREATE INDEX ON synopobservations (wigosid);
CREATE INDEX ON synopobservations (meta_received_datetime);


