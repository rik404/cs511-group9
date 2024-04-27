import sys
import pandas as pd
from pyspark.sql import SparkSession
import redis
spark = SparkSession.builder \
    .appName("Load CSV to Redis") \
    .getOrCreate()
def load_csv_to_dataframe(hdfs_path):
    return spark.read.csv(hdfs_path, header=True)
def format_row_to_redis_command(row):
    return ['HMSET', f"{row['l_shipmode']}_{row['o_orderkey']}_{row['o_orderstatus']}",
            'l_linenumber', row['l_linenumber'],
            'l_quantity', row['l_quantity'],
            'l_extendedprice', row['l_extendedprice'],
            'l_discount', row['l_discount'],
            'l_tax', row['l_tax'],
            'l_returnflag', row['l_returnflag'],
            'l_status', row['l_status'],
            'l_shipdate', row['l_shipdate'],
            'l_commitdate', row['l_commitdate'],
            'l_receiptdate', row['l_receiptdate'],
            'l_shipinstruct', row['l_shipinstruct'],
            'o_orderdate', row['o_orderdate'],
            'o_orderpriority', row['o_orderpriority'],
            'o_shippriority', row['o_shippriority'],
            'c_custkey', row['c_custkey'],
            'c_name', row['c_name'],
            'c_address', row['c_address'],
            'c_phone', row['c_phone'],
            'c_mktsegment', row['c_mktsegment'],
            'c_nationname', row['c_nationname'],
            'c_regionname', row['c_regionname'],
            'p_partkey', row['p_partkey'],
            'p_name', row['p_name'],
            'p_mfgr', row['p_mfgr'],
            'p_brand', row['p_brand'],
            'p_type', row['p_type'],
            'p_size', row['p_size'],
            'p_container', row['p_container'],
            'p_retailprice', row['p_retailprice'],
            's_suppkey', row['s_suppkey'],
            's_name', row['s_name'],
            's_address', row['s_address'],
            's_phone', row['s_phone'],
            's_nationname', row['s_nationname'],
            's_regionname', row['s_regionname']]
def execute_redis_commands(redis_commands):
    r = redis.Redis(host='redis511', port=6379, db=2)
    for cmd in redis_commands:
        r.execute_command(*cmd)
if len(sys.argv) != 2:
    print("Usage: spark-submit loadflat.py <num_files>")
    sys.exit(1)
try:
    num_files = int(sys.argv[1])
except ValueError:
    print("Error: Number of files must be an integer")
    sys.exit(1)
for i in range(1, num_files + 1):
    file_path = f"hdfs://main:9000/redis/flat_line_item_{i}.csv"
    print(f"Reading file: {file_path}")
    df = load_csv_to_dataframe(file_path)
    num_partitions = df.select("l_linenumber").distinct().count()
    df = df.repartition(num_partitions)
    redis_commands = df.rdd.map(format_row_to_redis_command).collect()
    execute_redis_commands(redis_commands)
    print(f"Contents of file {file_path} have been written to Redis.")
spark.stop()