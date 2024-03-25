#!/bin/bash

set -e

# Start ArangoDB server in the background
arangod --server.endpoint tcp://0.0.0.0:8529 --server.authentication false &

# Wait for ArangoDB to start
sleep 5

# Import the CSV file into ArangoDB
arangoimport --file /snow_date.csv --type csv --collection users --create-collection true
arangoimport --file /snow_line_item.csv --type csv --collection lineItems --create-collection true

# Keep the container running
tail -f /dev/null
