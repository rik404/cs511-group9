#!/bin/bash

# set -e

# # Start ArangoDB server in the background
# arangod --server.endpoint tcp://0.0.0.0:8529 --server.authentication false &

# # Wait for ArangoDB to start
# sleep 5

# # Import the CSV file into ArangoDB
# arangoimport --file /snow_date.csv --type csv --collection users --create-collection true
# arangoimport --file /snow_line_item.csv --type csv --collection lineItems --create-collection true

hdfs dfs -mkdir -p /user/spark
# hdfs dfs -put snow_date.csv /user/spark/snow_date.csv
# hdfs dfs -put snow_line_item.csv /user/spark/snow_line_item.csv
hdfs dfs -put flat_line_item.csv /user/spark/flat_line_item.csv

# spark-submit ./code/sparkProcessing.py --config ./code/config.yml --dataset snow_date.csv --parquet_file processed_data.parquet --collection_name sample
# spark-submit --driver-memory 5g ./code/sparkProcessing.py --config ./code/config.yml --dataset flat_line_item.csv --parquet_file processed_data.parquet --collection_name sample
spark-submit --driver-memory 5g --packages "com.arangodb:arangodb-spark-datasource-3.4_2.12:1.6.0" --class ArangoLoader ./code/sparkarangoloader_2.12-0.1.0-SNAPSHOT.jar


# Keep the container running
tail -f /dev/null
