docker-compose -f cs511p1-compose.yaml exec main bash -x -c spark-shell --packages org.apache.kudu:kudu-spark_2.10:1.5.0-cdh5.13.91 --repositories https://repository.cloudera.com/artifactory/cloudera-repos/


val df = spark.read
  .options(Map("kudu.master" -> "kudu-master:7051", "kudu.table" -> "kudu_table"))
  .format("kudu").load

  val kuduContext = new KuduContext("kudu-master:7051", spark.sparkContext)
kuduContext.createTable(
    "test_table", schema, Seq("Name"),
    new CreateTableOptions()
        .setNumReplicas(1)
        .addHashPartitions(List("Name").asJava, 3))

        import org.apache.kudu.client._
import collection.JavaConverters._

// Read a table from Kudu
val df = spark.read
  .options(Map("kudu.master" -> "kudu-master:7051", "kudu.table" -> "test_table"))
  .format("kudu").load

  spark-shell --packages org.apache.kudu:kudu-spark3_2.12:1.17.0