### Executing the PostgreSQL Data Pipeline

Generate the required dataset from koalabench using the following command:

```
java DBGen json snow sf<scale-factor-number>
```

Once the individual files have been generated, move them into `/Snow` directory.

#### Starting Docker

- Run `bash start-all.sh` as this will setup the docker cluster consisting of 1 master and 3 worker nodes. 
- This script will also install Hadoop 3.3.6 and Spark 3.4.0 on all three systems.

#### Setting up PostgreSQL Environment

Run `bash postgres_full.sh`. This script aims to do the following operations on your docker cluster:
- Pre-process customer, order and lineitem CSVs to remove any rows with discrepancies in foreign key attributes.
- Instantiates a psql container with the latest docker image and moves the psql jar to main system in the docker cluster.
- Sequentially copies all CSV files from local to main system and then loads them into HDFS of main system.
- Sequentially copies the PySpark scripts for loading the data into individual tables.

#### Loading Data into PostgreSQL

- Run `sparksubmit_dataload.sh` to sequentially load data onto every table in the database.
- The time taken for the entire process to complete is shown at the end of a successful execution and this can be recorded as the data loading time for the selected dataset.

#### Running Benchmark Queries

- Run `python3 postgres_execute_queries.py` to execute the selected benchmark queries from TPC-H collection. 
- At the end of every query, the received output along with time taken is displayed on shell.