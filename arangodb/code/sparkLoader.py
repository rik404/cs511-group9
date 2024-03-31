from pyArango.connection import Connection
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CSVProcessing") \
    .getOrCreate()

# Read CSV file from HDFS
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("hdfs://main:9000/user/spark/input.csv")

# Perform processing (e.g., transformations, aggregations)
# For example, you can show the DataFrame
df.show()

# Save the processed data to Parquet format in HDFS
df.write.parquet("hdfs://main:9000/user/spark/processed_data.parquet")

# Stop SparkSession
spark.stop()

# Connect to ArangoDB
conn = Connection(username="root", password="cs511grp9", arangoURL="http://arangodb:8529")
db = conn["_system"]

collection_name = "sample"
if collection_name not in db.collections:
    db.createCollection(name=collection_name)

collection = db[collection_name]

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ArangoDBUpload") \
    .getOrCreate()

# Read processed data from HDFS
processedData = spark.read.parquet("hdfs://main:9000/user/spark/processed_data.parquet")

# Convert Spark DataFrame to Pandas DataFrame
processed_df_pandas = processedData.toPandas()

# Convert Pandas DataFrame to list of dictionaries
data = processed_df_pandas.to_dict(orient="records")

# Insert data into ArangoDB collection
for doc in data:
    collection.createDocument(doc).save()

# Stop SparkSession
spark.stop()
