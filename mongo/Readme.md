Run start-all to start the container
Run pyspark_shell_master to connect to pyspark with mongo access mongo using :

# spark.read.format("mongodb").option( "spark.mongodb.input.uri", "mongodb://mongodb:27017/$db.$collection").load() to read a particular coll
# spark.write.format("mongodb").option("spark.mongodb.output.uri", "mongodb://mongodb:27017/$db.$collection").save() to write to particular coll
sed -i "1s/.*/lineNumber,quantity,extendedPrice,discount,tax,returnFlag,status,shipDate,commitDate,receiptDate,shipInstructions,shipMode,orderKey,orderStatus,orderDate,orderPriority,o_shipPriority,customerKey,c_name,c_address,c_phone,c_marketSegment,c_nation_name,c_region_name,partKey,p_name,p_manufacturer,p_brand,p_type,p_size,p_container,p_retailPrice,supplierKey,s_name,s_address,s_phone,s_nation_name,s_region_name/" flat_line_item.csv

read csv
df = spark.read.option("header", True).schema(schema).csv('/flat_line_item.csv')

mongo write
from datetime import datetime
def func():
    print(datetime.now())
    df.write.format("mongodb").option("spark.mongodb.output.uri", "mongodb://mongodb:27017/").option("database","test").option("collection","flat").mode("append").save()
    print(datetime.now())


mongo read
df2 = spark.read.format("mongodb").option( "spark.mongodb.input.uri", "mongodb://mongodb:27017/").option("database", "test").option("collection","flat").load()

spark.read.json(sc.wholeTextFiles("/flat_line_item.json").values().flatMap(lambda x: x.replace("\n", "#!#").replace("{#!# ", "{").replace("#!#}", "}").replace(",#!#", ",").split("#!#")))

TODO
Data cube link 

INSERT 45 gb - sf20
    Started - 2024-03-29 05:26:48
    End -  2024-03-29 09:00:37

INSERT 22.5 gb - sf10
    Started - 24/03/31 18:54:24
    End - 2024-03-31 20:20:02

INSERT 2.25 gb - sf1
    Started - 2024-04-03 00:50:24
    End - 2024-04-03 01:02:25

INSERT 11.2gb - sf5
    Started - 2024-04-09 03:40:54.275529
    End - 2024-04-09 04:27:44.487604

INSERT 6.7gb - sf3
    Started - 2024-04-09 05:07:06.987312
    End - 2024-04-09 05:35:28.105917
