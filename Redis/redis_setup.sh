#!/bin/bash
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <flat_lineitem_path>"
    exit 1
fi
echo "Instantiating Redis Container"
docker pull redis:latest
docker run --name redis511 --network postgres_default -p 6379:6379 -d redis
echo "Adding Linenumber ID to Dataset"
python3 addingLineNumberID.py "$1"
printf '%.s=' {1..25}
printf '%.s=' {1..25}
echo "Loading data to HDFS"
docker compose -f cs511p1-compose.yaml cp modified_flat_line_item.csv main:/flat_line_item.csv
docker compose -f cs511p1-compose.yaml cp spark_dataload.py main:/spark_dataload.py
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'hdfs dfs -mkdir -p /redis && hdfs dfs -chmod 777 /redis && hdfs dfs -put -f flat_line_item.csv /redis/flat_line_item.csv'
printf '%.s=' {1..25}
echo "Loading data onto Spark from HDFS"
start_time=$(date +%s)  # Capture the start time
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'spark-submit spark_dataload.py -driver 3G'
wait
end_time=$(date +%s)
printf '%.s=' {1..25}
echo "Time taken: $((end_time - start_time)) seconds"  # Calculate and print the time taken
printf '%.s=' {1..25}
printf '%.s=' {1..25}