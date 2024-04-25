hdfs dfs -mkdir -p /user/spark

hdfs dfs -put flat_line_item.csv /user/spark/flat_line_item.csv

spark-submit --driver-memory 5g --packages "com.arangodb:arangodb-spark-datasource-3.4_2.12:1.6.0" --class ArangoLoader ./code/sparkarangoloader_2.12-0.1.0-SNAPSHOT.jar

tail -f /dev/null
