Run start-all to start the container
Run pyspark_shell_master to connect to pyspark with mongo access mongo using :

# spark.read.format("mongodb").option( "spark.mongodb.input.uri", "mongodb://mongodb:27017/$db.$collection").load() to read a particular coll
# spark.write.format("mongodb").option("spark.mongodb.output.uri", "mongodb://mongodb:27017/$db.$collection").save() to write to particular coll

read csv
df = spark.read.option("header", True).schema(schema).csv('/flat_line_item.csv')

mongo write 
df.write.format("mongodb").option("spark.mongodb.output.uri", "mongodb://mongodb:27017/").option("database","test").option("collection","flat").mode("append").save()

mongo read
df2 = spark.read.format("mongodb").option( "spark.mongodb.input.uri", "mongodb://mongodb:27017/").option("database", "test").option("collection","flat").load()

spark.read.json(sc.wholeTextFiles("/flat_line_item.json").values().flatMap(lambda x: x.replace("\n", "#!#").replace("{#!# ", "{").replace("#!#}", "}").replace(",#!#", ",").split("#!#")))

TODO
Data cube link 

INSERT 45 gb - sf20
    Started - 2024-03-29 05:26:48
    End -  2024-03-29 09:00:37
