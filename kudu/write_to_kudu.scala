import org.apache.kudu.client._
import collection.JavaConverters._
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._ 

val schema = StructType(
  Array(
    StructField("lineNumber", IntegerType, nullable = true),
    StructField("quantity", IntegerType, nullable = true),
    StructField("extendedPrice", FloatType, nullable = true),
    StructField("discount", FloatType, nullable = true),
    StructField("tax", FloatType, nullable = true),
    StructField("returnFlag", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("shipDate", DateType, nullable = true),
    StructField("commitDate", DateType, nullable = true),
    StructField("receiptDate", DateType, nullable = true),
    StructField("shipInstructions", StringType, nullable = true),
    StructField("shipMode", StringType, nullable = true),
    StructField("orderKey", IntegerType, nullable = true),
    StructField("orderStatus", StringType, nullable = true),
    StructField("orderDate", DateType, nullable = true),
    StructField("orderPriority", StringType, nullable = true),
    StructField("o_shipPriority", IntegerType, nullable = true),
    StructField("customerKey", IntegerType, nullable = true),
    StructField("c_name", StringType, nullable = true),
    StructField("c_address", StringType, nullable = true),
    StructField("c_phone", StringType, nullable = true),
    StructField("c_marketSegment", StringType, nullable = true),
    StructField("c_nation_name", StringType, nullable = true),
    StructField("c_region_name", StringType, nullable = true),
    StructField("partKey", IntegerType, nullable = true),
    StructField("p_name", StringType, nullable = true),
    StructField("p_manufacturer", StringType, nullable = true),
    StructField("p_brand", StringType, nullable = true),
    StructField("p_type", StringType, nullable = true),
    StructField("p_size", StringType, nullable = true),
    StructField("p_container", StringType, nullable = true),
    StructField("p_retailPrice", FloatType, nullable = true),
    StructField("supplierKey", IntegerType, nullable = true),
    StructField("s_name", StringType, nullable = true),
    StructField("s_address", StringType, nullable = true),
    StructField("s_phone", StringType, nullable = true),
    StructField("s_nation_name", StringType, nullable = true),
    StructField("s_region_name", StringType, nullable = true)
  )
)
val df = spark.read.option("headers",true).schema(schema).csv("hdfs://flat_line_item.csv").withColumn("id",monotonicallyIncreasingId)
val schema1 = schema.add(StructField("id", LongType, nullable = false))
val kuduContext = new KuduContext("kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251", spark.sparkContext)

kuduContext.createTable("line_item", schema1, Seq("id"),new CreateTableOptions().setNumReplicas(1).addHashPartitions(List("id").asJava, 3))
kuduContext.insertRows(df, "line_item")

kuduContext.deleteTable("line_item")

df.write.options(Map("kudu.master" -> "kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251", "kudu.table" -> "line_item")).format("kudu").mode("append").save()