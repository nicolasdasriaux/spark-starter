package sparkstarter

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkStarterApp {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Spark Starter App")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    implicit val sc: SparkContext = spark.sparkContext

    SparkStarter.keepSparkUIAlive()

    spark.close()
  }
}
