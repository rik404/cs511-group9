# Deprecated Using Scala jar for loading data into arangoDB

from pyArango.connection import Connection
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("CSVProcessing") \
    .getOrCreate()

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("hdfs://main:9000/user/spark/input.csv")

df.show()

df.write.parquet("hdfs://main:9000/user/spark/processed_data.parquet")

spark.stop()

conn = Connection(username="root", password="cs511grp9", arangoURL="http://arangodb:8529")
db = conn["_system"]

collection_name = "sample"
if collection_name not in db.collections:
    db.createCollection(name=collection_name)

collection = db[collection_name]

spark = SparkSession.builder \
    .appName("ArangoDBUpload") \
    .getOrCreate()

processedData = spark.read.parquet("hdfs://main:9000/user/spark/processed_data.parquet")

processed_df_pandas = processedData.toPandas()

data = processed_df_pandas.to_dict(orient="records")

for doc in data:
    collection.createDocument(doc).save()

spark.stop()
