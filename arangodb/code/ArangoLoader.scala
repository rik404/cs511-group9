import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame


object ArangoLoader {
  def main(args: Array[String]): Unit = {
    // Step 1: Create SparkSession
    val spark = SparkSession.builder()
      .appName("Load CSV to ArangoDB")
      .config("spark.arangodb.database", "_system") // Set ArangoDB database
      .config("spark.arangodb.collection", "user") // Set ArangoDB collection
      .config("spark.arangodb.createCollection", "true") // Create collection if it doesn't exist
      .config("spark.arangodb.host", "arangodb") // Set ArangoDB host
      .getOrCreate()

    try {
      // Step 2: Read Data from HDFS
//      val inputDF = readData(spark, "hdfs://main:9000/user/spark/flat_line_item.csv")

      val inputDF = spark.read.format("csv").option("header", "true").load("hdfs://main:9000/user/spark/flat_line_item.csv")
      // Step 3: Data Processing (if necessary)

      // Step 4: Write Data to ArangoDB
      writeDataToArangoDB(inputDF)
    } finally {
      // Step 5: Stop SparkSession
      spark.stop()
    }
  }

  // Step 2: Read Data from HDFS
  def readData(spark: SparkSession, path: String): DataFrame = {
    spark.read.format("csv").option("header", "true").load(path)
  }

  // Step 4: Write Data to ArangoDB
  def writeDataToArangoDB(dataFrame: DataFrame): Unit = {
    dataFrame.write.format("com.arangodb.spark").mode("append").options(Map("password" -> "cs511grp9", "endpoints" -> "arangodb:8529", "database" -> "_system", "table" -> "sample2")).save()
  }
}

