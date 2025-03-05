import org.apache.spark.sql.SparkSession

object SimpleSparkJob {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Basic Spark Job")
      .master("local[*]") // Change to "yarn" when running on Cloudera
      .getOrCreate()

    // Create Sample Data
    val data = Seq(
      (1, "Alice", 25),
      (2, "Bob", 30),
      (3, "Charlie", 28)
    )

    // Convert Data to DataFrame
    import spark.implicits._
    val df = data.toDF("id", "name", "age")

    // Show Data
    df.show()

    // Stop Spark Session
    spark.stop()
  }
}
