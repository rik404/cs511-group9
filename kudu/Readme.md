## Kudu pipeline

### Dataset generation

Generate the required dataset from koalabench using the following command:

```
java DBGen csv flat sf<scale-factor-number>
```

Once the dataset has been generated move it into the Kudu root directory.

### Starting kudu-spark containers

Run `bash start-all.sh` to start the container. You might need to tweak your docker setup to allow env varaible passing to shell. Alternatively you can set the required KUDU IP using 

```
export KUDU_QUICKSTART_IP=$(ifconfig | grep "inet " | grep -Fv 127.0.0.1 |  awk '{print $2}' | tail -1) && \
```

### Connecting to spark shell with kudu access

Run `bash spark_shell_master.sh` to connect to spark with kudu access

### CSV header cleanup

The koalabench might create dataset with wrong headers. Use the below to correct it

```
sed -i "1s/.*/lineNumber,quantity,extendedPrice,discount,tax,returnFlag,status,shipDate,commitDate,receiptDate,shipInstructions,shipMode,orderKey,orderStatus,orderDate,orderPriority,o_shipPriority,customerKey,c_name,c_address,c_phone,c_marketSegment,c_nation_name,c_region_name,partKey,p_name,p_manufacturer,p_brand,p_type,p_size,p_container,p_retailPrice,supplierKey,s_name,s_address,s_phone,s_nation_name,s_region_name/" flat_line_item.csv
```

### Data load script

You can use the `write_to_kudu.scala` to load data from hadoop to kudu either through spark shell or spark submit.

### Setting up Impala shell

Run the below to start an impala container and connect it with kudu

`docker run -d --name kudu-impala  -p 21000:21000 -p 21050:21050 -p 25000:25000 -p 25010:25010 -p 25020:25020 --memory=4096m --network=docker_localnet apache/kudu:impala-latest impala`

Running the below will connect to the impala shell

`docker exec -it kudu-impala impala-shell`

### Accessing kudu from impala shell

```
CREATE EXTERNAL TABLE line_item
STORED AS KUDU
TBLPROPERTIES (
  'kudu.table_name' = 'line_item',
  'kudu.master_addresses' = 'kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251'
);
```
this will create an external table in impala and link it with existing kudu table

### Querying using impala shell

Although impala queries have slight limitations, most SQL queries should run fine. Run queries from `benchmark-queries.txt` for the benchmark process