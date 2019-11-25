package sparkstarter

import com.holdenkarau.spark.testing.RDDComparisons
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SparkStarterRddSpec extends FlatSpec with Matchers with BeforeAndAfterAll with RDDComparisons {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("RDD")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL
    .getOrCreate()

  val sc: SparkContext = sparkSession.sparkContext

  override def afterAll() {
    SparkStarter.keepSparkUIAlive()
    sparkSession.stop()
  }

  "RDD" should "trigger execution only when using an action" in {
    val numbersRDD = sc.parallelize(1 to 100000)
      .filter(i => i % 3 == 0)
      .map(i => s"Number $i")

    numbersRDD.collect()
  }
}
