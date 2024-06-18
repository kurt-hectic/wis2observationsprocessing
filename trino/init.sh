#!/bin/bash

# blocks until trino is reachable
while ! trino --server trino:8080 --execute 'SHOW CATALOGS' | grep "system"; do
    echo "trino is not reachable, sleeping"
    sleep 5
    done


# wait until there is data in the topic
while trino --server trino:8080 --execute 'select count(*) from kafka.default."notifications-tostorage" ' | grep -e '"0"'; do
    echo "No data in the topic, sleeping"
    sleep 5
    done 

echo -e 'Creating trino catalog'
cat /init.sql | trino --server trino:8080

echo -e 'Successfully created the following catalogs:'
trino --server trino:8080 --execute 'SHOW CATALOGS'