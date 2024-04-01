export KUDU_QUICKSTART_IP=$(ifconfig | grep "inet " | grep -Fv 127.0.0.1 |  awk '{print $2}' | tail -1) && \
docker compose -f docker/quickstart.yml cp flat_line_item.csv main:/flat_line_item.csv && \
docker-compose -f docker/quickstart.yml exec main bash -x -c 'spark-shell --packages org.apache.kudu:kudu-spark3_2.12:1.17.0'


# kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251

# val df = spark.read
#   .options(Map("kudu.master" -> "kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251", "kudu.table" -> "kudu_table"))
#   .format("kudu").load

#   val kuduContext = new KuduContext("kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251", spark.sparkContext)
# kuduContext.createTable("test_table", schema, Seq("Name"),new CreateTableOptions().setNumReplicas(1).addHashPartitions(List("Name").asJava, 3))

#         import org.apache.kudu.client._
# import collection.JavaConverters._

# // Read a table from Kudu
# val df = spark.read
#   .options(Map("kudu.master" -> "kudu-master:7051", "kudu.table" -> "test_table"))
#   .format("kudu").load()

#   spark-shell --packages org.apache.kudu:kudu-spark3_2.12:1.17.0

# val schema = StructType(
#   Seq(
#     StructField("Name", StringType, nullable = false),
#     StructField("Age", IntegerType, nullable = true),
#     StructField("City", StringType, nullable = true)
#   )
# )

# val df = spark.read
#   .options(Map("kudu.master" -> "kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251", "kudu.table" -> "test_table"))
#   .format("kudu").load()