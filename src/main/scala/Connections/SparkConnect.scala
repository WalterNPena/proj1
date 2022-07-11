package Connections
import org.apache.spark.sql.SparkSession

object SparkConnect {
  def createSparkSession(): SparkSession = {
    // Create a Spark Session
    // For Windows
    System.setProperty("hadoop.home.dir", "C:\\Hadoop3")

    val spark = SparkSession
      .builder
      .appName("HelloP1")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark
  }
}
