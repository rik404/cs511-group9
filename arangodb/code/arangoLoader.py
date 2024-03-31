from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from pyArango.connection import Connection
import json
# Step 1: Create a Spark session
spark = SparkSession.builder \
    .appName("CSV to ArangoDB") \
    .getOrCreate()

# Step 2: Read the CSV file into a DataFrame
df = spark.read.csv("snow_date.csv", header=True, inferSchema=True)

# Step 3: Convert DataFrame to JSON
# json_df = df.select(to_json("*").alias("json"))
json_df = df.select(to_json(struct(df.columns)).alias("json"))

# Step 4: Write JSON data to ArangoDB
conn = Connection(username="root", password="cs511grp9", arangoURL="http://arangodb:8529")
db = conn["_system"]

collection_name = "sample"
if collection_name not in db.collections:
    db.createCollection(name=collection_name)

collection = db[collection_name]

for row in json_df.collect():
    document = row.json
    try:
        document_dict = json.loads(document)
        collection.createDocument(document_dict).save()
    except json.JSONDecodeError:
        print("Error: Invalid JSON format:", document)

# Step 5: Stop Spark session
spark.stop()
