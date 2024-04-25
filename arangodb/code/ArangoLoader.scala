import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame


object ArangoLoader {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Load CSV to ArangoDB")
      .config("spark.arangodb.database", "_system") 
      .config("spark.arangodb.collection", "user") 
      .config("spark.arangodb.createCollection", "true") 
      .config("spark.arangodb.host", "arangodb") 
      .getOrCreate()

    try {
      
//      val inputDF = readData(spark, "hdfs://main:9000/user/spark/flat_line_item.csv")

      val inputDF = spark.read.format("csv").option("header", "true").load("hdfs://main:9000/user/spark/flat_line_item.csv")
      writeDataToArangoDB(inputDF)
      
    } finally {
      spark.stop()
    }
  }

  def readData(spark: SparkSession, path: String): DataFrame = {
    spark.read.format("csv").option("header", "true").load(path)
  }

  def writeDataToArangoDB(dataFrame: DataFrame): Unit = {
    dataFrame.write.format("com.arangodb.spark").mode("append").options(Map("password" -> "cs511grp9", "endpoints" -> "arangodb:8529", "database" -> "_system", "table" -> "sample2")).save()
  }
}

