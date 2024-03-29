docker compose -f cs511p1-compose.yaml cp flat_line_item.csv main:/flat_line_item.csv
docker-compose -f cs511p1-compose.yaml exec main bash -x -c 'pyspark --conf "spark.mongodb.read.connection.uri=mongodb://mongodb:27017/spark.times?readPreference=primaryPreferred" \
              --conf "spark.mongodb.write.connection.uri=mongodb://mongodb:27017/spark.test_db" \
              --driver-memory 5g \
              --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1'
