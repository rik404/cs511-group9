# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pandas as pd
from pyArango.connection import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Load CSV to ArangoDB") \
    .getOrCreate()

# Read CSV from HDFS into PySpark DataFrame
def read_csv_from_hdfs(file_path: str) -> DataFrame:
    return spark.read.csv(file_path, header=True, inferSchema=True)

# Connect to ArangoDB and insert data
def insert_data_to_arangodb(df: DataFrame, database_name: str, collection_name: str, chunk_size: int):
    conn = Connection(arangoURL="http://172.23.0.6:8529", username="root", password="Krishna1497")
    db = conn.createDatabase(name=database_name)
    
    # Create a new collection if it does not exist
    if collection_name not in db.collections:
        collection = db.createCollection(name=collection_name)
    else:
        collection = db[collection_name]
    
    print("Inserting data into ArangoDB in chunks...")
    total_rows = df.count()
    for i in range(0, total_rows, chunk_size):
        chunk_df = df.limit(chunk_size).toPandas()
        for index, row in chunk_df.iterrows():
            doc = collection.createDocument()
            doc._key = str(index)
            for column, value in row.items():
                doc[column] = value
            doc.save()
        print(f"Processed {min(i + chunk_size, total_rows)} out of {total_rows} rows")
    
    print("Data insertion completed!")

# Main function to orchestrate the process
def main():
    # Define file path
    hdfs_file_path = "hdfs://main:9000/arango/covid_data_usa.csv"
    
    # Read CSV from HDFS
    df = read_csv_from_hdfs(hdfs_file_path)
    
    # Connect to ArangoDB and insert data
    insert_data_to_arangodb(df, database_name="arango_test", collection_name="covid_usa", chunk_size=1000)
    
    print("Process completed successfully!")

if __name__ == "__main__":
    main()