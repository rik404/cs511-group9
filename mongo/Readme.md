## Mongo pipeline

### Dataset generation

Generate the required dataset from koalabench using the following command:

```
java DBGen csv flat sf<scale-factor-number>
```

Once the dataset has been generated move it into the Mongo root directory.

### Starting mongo-spark containers

Run `bash start-all.sh` to start the container

### Connecting to pyspark shell with mongo access

Run `bash pyspark_shell_master.sh` to connect to pyspark with mongo access

### CSV header cleanup

The koalabench might create dataset with wrong headers. Use the below to correct it

```
sed -i "1s/.*/lineNumber,quantity,extendedPrice,discount,tax,returnFlag,status,shipDate,commitDate,receiptDate,shipInstructions,shipMode,orderKey,orderStatus,orderDate,orderPriority,o_shipPriority,customerKey,c_name,c_address,c_phone,c_marketSegment,c_nation_name,c_region_name,partKey,p_name,p_manufacturer,p_brand,p_type,p_size,p_container,p_retailPrice,supplierKey,s_name,s_address,s_phone,s_nation_name,s_region_name/" flat_line_item.csv
```

### Spark read from csv

```
df = spark.read.option("header", True).schema(schema).csv('hdfs://flat_line_item.csv')
```

### Loading and reading data from mongo
`spark.read.format("mongodb").option( "spark.mongodb.input.uri", "mongodb://mongodb:27017/$db.$collection").load()`
to read a particular collection

`spark.write.format("mongodb").option("spark.mongodb.output.uri", "mongodb://mongodb:27017/$db.$collection").save()` to write to particular collection

### Timed write to estimate load times

```python
from datetime import datetime
def func():
    print(datetime.now())
    df.write.format("mongodb").option("spark.mongodb.output.uri", "mongodb://mongodb:27017/").option("database","test").option("collection","flat").mode("append").save()
    print(datetime.now())
```
Call this function within the spark shell

### Mongo query execution

Run `bash mongoshell.sh` to get into the mongo shell and use queries from benchmark-queries.txt
