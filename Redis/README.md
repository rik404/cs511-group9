### Executing the Redis Data Pipeline

Generate the required dataset from koalabench using the following command:

```
java DBGen json flat sf<scale-factor-number>
```

Once the dataset has been generated move it into the Redis root directory.

#### Starting Docker

- Run `bash start-all.sh` as this will setup the docker cluster consisting of 1 master and 3 worker nodes. 
- This script will also install Hadoop 3.3.6 and Spark 3.4.0 on all three systems.

#### Setting up Redis Environment

Run `bash redis_setup.sh`. This script aims to do the following operations on your docker cluster:
- Spawn a redis container with the latest version.
- Pre-processes the dataset and prepares it for data loading.
- Moves the pre-processed dataset from local to HDFS.
- Executes the PySpark script `spark_dataload.py` to load data from HDFS to Redis Container.
- The time taken by the cluster to complete the loading of data is shown at the end of the script's execution.

#### Running Benchmark Queries

- Run `bash redis_queries.sh` to execute the selected benchmark queries from TPC-H collection. 
- All queries are available under `/benchmark_queries`.
- At the end of every query, the received output along with time taken is displayed on shell.