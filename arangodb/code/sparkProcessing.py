import argparse
import yaml
from pyArango.connection import Connection
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType

def read_config(config_file):
    with open(config_file, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
            return config
        except yaml.YAMLError as exc:
            print(exc)

def process_csv_and_upload(config_file, parquet_file, collection_name, dataset):
    # Read configuration from YAML file
    config = read_config(config_file)
    hdfs_host = config['hdfs']['host']
    hdfs_port = config['hdfs']['port']
    arango_username = config['arango']['username']
    arango_password = config['arango']['password']
    arango_host = config['arango']['host']
    arango_port = config['arango']['port']
    arango_db = config['arango']['db']

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("CSVProcessing") \
        .getOrCreate()

    schema = StructType([
        StructField('lineNumber', IntegerType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('extendedPrice', FloatType(), True),
        StructField('discount', FloatType(), True),
        StructField('tax', FloatType(), True),
        StructField('returnFlag', StringType(), True),
        StructField('status', StringType(), True),
        StructField('shipDate', DateType(), True),
        StructField('commitDate', DateType(), True),
        StructField('receiptDate', DateType(), True),
        StructField('shipInstructions', StringType(), True),
        StructField('shipMode', StringType(), True),
        StructField('orderKey', IntegerType(), True),
        StructField('o_orderStatus', StringType(), True),
        StructField('o_orderDate', DateType(), True),
        StructField('o_orderPriority', StringType(), True),
        StructField('o_shipPriority', IntegerType(), True),
        StructField('customerKey', IntegerType(), True),
        StructField('c_name', StringType(), True),
        StructField('c_address', StringType(), True),
        StructField('c_phone', StringType(), True),
        StructField('c_marketSegment', StringType(), True),
        StructField('c_nation_name', StringType(), True),
        StructField('c_region_name', StringType(), True),
        StructField('partKey', IntegerType(), True),
        StructField('p_name', StringType(), True),
        StructField('p_manufacturer', StringType(), True),
        StructField('p_brand', StringType(), True),
        StructField('p_type', StringType(), True),
        StructField('p_size', StringType(), True),
        StructField('p_container', StringType(), True),
        StructField('p_retailPrice', FloatType(), True),
        StructField('supplierKey', IntegerType(), True),
        StructField('s_name', StringType(), True),
        StructField('s_address', StringType(), True),
        StructField('s_phone', StringType(), True),
        StructField('s_nation_name', StringType(), True),
        StructField('s_region_name', StringType(), True)
    ])
    # Read CSV file from HDFS
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema)\
        .load(f"hdfs://{hdfs_host}:{hdfs_port}/user/spark/{dataset}")

    # Perform processing (e.g., transformations, aggregations)
    # For example, you can show the DataFrame
    df.show()

    # Save the processed data to Parquet format in HDFS
    # df.write.parquet(f"hdfs://{hdfs_host}:{hdfs_port}/user/spark/{parquet_file}")

    # Stop SparkSession
    # spark.stop()

    # Connect to ArangoDB

    conn = Connection(username=arango_username, password=arango_password, arangoURL=f"http://{arango_host}:{arango_port}")
    db = conn[arango_db]

    if collection_name not in db.collections:
        db.createCollection(name=collection_name)

    collection = db[collection_name]

    # properties = {
    #     "user": arango_username,
    #     "password": arango_password,
    #     "driver": "com.arangodb.jdbc.ArangoDriver"
    # }

    # df.write.jdbc(url=f"http://{arango_host}:{arango_port}", table=collection_name, mode="overwrite", properties=properties)

    # Initialize SparkSession
    # spark = SparkSession.builder \
    #     .appName("ArangoDBUpload") \
    #     .getOrCreate()

    # Read processed data from HDFS
    # processedData = spark.read.parquet(f"hdfs://{hdfs_host}:{hdfs_port}/user/spark/{parquet_file}")

    # Convert Spark DataFrame to Pandas DataFrame
    # processed_df_pandas = processedData.toPandas()
    # processed_df_pandas = df.toPandas()

    # Convert Pandas DataFrame to list of dictionaries
    # data = processed_df_pandas.to_dict(orient="records")

    data = df.collect()
    # Insert data into ArangoDB collection
    # for doc in data:
    #     collection.createDocument(doc).save()

    for row in data:
        doc = {field.name: getattr(row, field.name) for field in df.schema.fields}
        collection.createDocument(doc).save()
    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process CSV and upload to ArangoDB")
    parser.add_argument("--config", help="Path to YAML configuration file")
    parser.add_argument("--parquet_file", help="Name for the Parquet file")
    parser.add_argument("--collection_name", help="Name for the ArangoDB collection")
    parser.add_argument("--dataset", help = "the datafile to be loaded")
    args = parser.parse_args()

    process_csv_and_upload(args.config, args.parquet_file, args.collection_name, args.dataset)
    print("****** Exited the code ******")
