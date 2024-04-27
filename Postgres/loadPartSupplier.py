from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
spark = SparkSession.builder \
    .appName("CSV to PostgreSQL") \
    .getOrCreate()
hdfs_host = "main"
hdfs_port = "9000"
hdfs_path = "/postgres/snow_part_supplier.csv"
postgres_host = "postgres511"
postgres_port = "5432"
database_name = "cs511"
table_name = "PartSupplier"
username = "postgres"
password = "cs511"
jdbc_driver = "org.postgresql.Driver"
schema = StructType([
    StructField("ps_partkey", IntegerType(), False),
    StructField("ps_suppkey", IntegerType(), True),
    StructField("ps_availqty", IntegerType(), True),
    StructField("ps_supplycost", FloatType(), True),
    StructField("ps_comment", StringType(), True)
])
csv_file_path = f"hdfs://{hdfs_host}:{hdfs_port}{hdfs_path}"
df = spark.read.csv(csv_file_path, schema=schema, header=True)
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