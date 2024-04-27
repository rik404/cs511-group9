from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
spark = SparkSession.builder \
    .appName("CSV to PostgreSQL") \
    .getOrCreate()
hdfs_host = "main"
hdfs_port = "9000"
hdfs_path = "/postgres/snow_date.csv"
postgres_host = "postgres511"
postgres_port = "5432"
database_name = "cs511"
table_name = "Date"
username = "postgres"
password = "cs511"
jdbc_driver = "org.postgresql.Driver"
csv_file_path = f"hdfs://{hdfs_host}:{hdfs_port}{hdfs_path}"
df = spark.read.csv(csv_file_path, header=True)
df = df.withColumn("dateKey", monotonically_increasing_id())
column_order = ["dateKey"] + [col_name for col_name in df.columns if col_name != "dateKey"]
df = df.select(column_order)
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