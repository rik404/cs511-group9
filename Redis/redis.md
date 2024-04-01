### HDFS:
- Copy from local to main.
- Use hdfs dfs to create a directory with /.
- chmod 777 for that directory.
- copy from main to hdfs location.
- do hdfs dfs -ls to see list of transferred and available files on HDFS.

### Redis:
- Create redis as a separate container.
- Add the new container to the existing master-slave network. (Beaware of hostname)

### Spark:
- Start spark shell session
- Read from CSV.
- Create an struct if requried.
- Copy partition by partition.
- Check the corresponding key to see if all values have been written to redis.