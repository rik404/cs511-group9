from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
spark = SparkSession.builder \
    .appName("CSV to PostgreSQL") \
    .getOrCreate()
hdfs_host = "main"
hdfs_port = "9000"
hdfs_path = "/postgres/snow_line_item.csv"
postgres_host = "postgres511"
postgres_port = "5432"
database_name = "cs511"
table_name = "LineItem"
username = "postgres"
password = "cs511"
jdbc_driver = "org.postgresql.Driver"
num_partitions = 6
schema = StructType([
    StructField("l_orderkey", IntegerType(), True),
    StructField("l_partkey", IntegerType(), True),
    StructField("l_suppkey", IntegerType(), True),
    StructField("l_linenumber", IntegerType(), True),
    StructField("l_quantity", IntegerType(), True),
    StructField("l_extendedprice", FloatType(), True),
    StructField("l_discount", FloatType(), True),
    StructField("l_tax", FloatType(), True),
    StructField("l_returnflag", StringType(), True),
    StructField("l_linestatus", StringType(), True),
    StructField("l_shipdate", DateType(), True),
    StructField("l_commitdate", DateType(), True),
    StructField("l_receiptdate", DateType(), True),
    StructField("l_shipinstruct", StringType(), True),
    StructField("l_shipmode", StringType(), True),
    StructField("l_comment", StringType(), True)
])
csv_file_path = f"hdfs://{hdfs_host}:{hdfs_port}{hdfs_path}"
df = spark.read.csv(csv_file_path, schema=schema, header=True) \
    .repartition(num_partitions, "l_shipmode")
postgres_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{database_name}"
properties = {
    "user": username,
    "password": password,
    "driver": jdbc_driver
}
df.write \
    .mode("overwrite") \
    .jdbc(postgres_url, table_name, properties=properties)
spark.stop()