Run start-all to start the container
Run pyspark_shell_master to connect to pyspark with mongo access mongo using :

# spark.read.format("mongodb").option( "spark.mongodb.input.uri", "mongodb://mongodb:27017/$db.$collection").load() to read a particular coll
# spark.write.format("mongodb").option("spark.mongodb.output.uri", "mongodb://mongodb:27017/$db.$collection").save() to write to particular coll

TODO
Data cube link 