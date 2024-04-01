from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

def main(spark, output_path):
    err_code = 1
    try:
        print("<<<<<<Attempting to read>>>>>>>")
        csv_file_path = "hdfs://main:9000/Sorting/TerraSort_Cap.csv"
        df = spark.read.csv(csv_file_path, header=True)
        print("<<<<<<<<<<Input file ready>>>>>>>>>>>")
        df.show(n=5)
        filtered_df = df.filter(col("Year of manufacture") > 2023)
        sorted_df = filtered_df.orderBy(col("Year of manufacture").desc(), col("Serial number"))
        # output_path = "TerraSort_Cap_Sorted_OP.csv"
        # print("<<<<<<<<<<Output file ready>>>>>>>>>>>")
        # sorted_df.coalesce(1).write.csv(f"{output_path}",header=False,mode='overwrite') ## df is an existing DataFrame object.) ## df is an existing DataFrame object.
        # sorted_df.show(n=5)
        # print(f"<<<<<<<<<<Output file ready at {output_path} >>>>>>>>>>>")
        # print("<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        pre_sorted_file = "hdfs://main:9000/Sorting/TerraSort_Cap_PreSorted.csv"
        pre_sorted_df = spark.read.csv(pre_sorted_file, header=False)
        print("<<<<<<<<<<Pre-Sorted File Ready>>>>>>>>>>>")
        if sorted_df.exceptAll(pre_sorted_df).count() == 0:
            print("The sorted DataFrame matches the pre-sorted DataFrame.")
            err_code = 0
        else:
            print("The sorted DataFrame does not match the pre-sorted DataFrame.")
            err_code = 1
            
    except Exception as e:
        print("An error occurred:", str(e))
        err_code = 1
    finally:
        spark.stop()
        sys.exit(err_code)


if __name__ == '__main__':
    spark = SparkSession.builder \
    .appName("TerraSort Caps") \
    .getOrCreate()
    if len(sys.argv) < 1:
        print("No arguments supplied")
        sys.exit(1)
    main(spark, sys.argv[1])