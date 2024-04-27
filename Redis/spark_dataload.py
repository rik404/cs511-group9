import time
from pyspark.sql import SparkSession
import redis
spark = SparkSession.builder \
    .appName("Load CSV to Redis") \
    .getOrCreate()
def load_csv_to_dataframe(hdfs_path):
    return spark.read.csv(hdfs_path, header=True)
def execute_redis_command(row):
    r = redis.Redis(host='redis511', port=6379, db=2)
    key = row['linenumber_id']
    fields = [str(column) for column in row.__fields__ if column != 'linenumber_id']
    values = [str(row[column]) for column in row.__fields__ if column != 'linenumber_id']
    r.hmset(key, dict(zip(fields, values)))
file_path = "hdfs://main:9000/redis/flat_line_item.csv"
start_time = time.time()
df = load_csv_to_dataframe(file_path)
df.rdd.map(execute_redis_command).collect()
end_time = time.time()
total_time = end_time - start_time
print("Total time taken for loading data:", total_time, "seconds")
spark.stop()