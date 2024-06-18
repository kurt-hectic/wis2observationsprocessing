#!/bin/bash

if trino --server trino:8080 --execute 'select count(*) from postgresql.public."kafka_table_offsets"' |  grep -ve '"0"' ; then
    echo "executing ETL job"
    cat /etl.sql | trino --server trino:8080
else
    echo "trino schema not yet initialized"
fi