#!/bin/bash

# Record start time
start=$(date +%s)

# Execute Spark-submit commands
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'spark-submit --jars postgresql-42.7.3.jar loadRegion.py'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'spark-submit --jars postgresql-42.7.3.jar loadNation.py'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'spark-submit --jars postgresql-42.7.3.jar loadPart.py'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'spark-submit --jars postgresql-42.7.3.jar loadCustomer.py'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'spark-submit --jars postgresql-42.7.3.jar loadSupplier.py'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'spark-submit --jars postgresql-42.7.3.jar loadOrder.py'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'spark-submit --jars postgresql-42.7.3.jar loadPartSupplier.py'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'spark-submit --jars postgresql-42.7.3.jar loadLineItem.py'

# Calculate elapsed time
end=$(date +%s)
runtime=$((end-start))

# Print elapsed time
echo "Total time taken: $runtime seconds"