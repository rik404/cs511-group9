from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV to PostgreSQL") \
    .getOrCreate()

# Read CSV file from HDFS into DataFrame
hdfs_host = "main"
hdfs_port = "9000"
hdfs_path = "/postgres/covid_data_usa.csv"
csv_file_path = f"hdfs://{hdfs_host}:{hdfs_port}{hdfs_path}"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show DataFrame schema and sample data
df.printSchema()
df.show()

# Transform and clean data if needed
# For example, you can rename columns, filter rows, handle missing values, etc.

# Write DataFrame to PostgreSQL
postgres_host = "172.24.0.6"  # Update with the container's IP address
postgres_port = "5432"  # Update with the exposed PostgreSQL port
database_name = "test511"  # Use the name of your database
postgres_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{database_name}"
properties = {
    "user": "postgres",  # Assuming default username is 'postgres'
    "password": "cs511project",  # Assuming password is 'cs511project'
    "driver": "org.postgresql.Driver"
}
table_name = "covid_data"

df.write \
    .mode("overwrite") \
    .jdbc(postgres_url, table_name, properties=properties)

# Stop Spark session
spark.stop()
