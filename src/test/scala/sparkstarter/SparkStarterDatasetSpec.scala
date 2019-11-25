package sparkstarter

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import sparkstarter.ecommerce.{Customer, ECommerce, Order}

class SparkStarterDatasetSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Dataset")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 20) // Parallelism when shuffling in Spark SQL
    .getOrCreate()

  implicit val sc: SparkContext = spark.sparkContext

  override def afterAll() {
    SparkStarter.keepSparkUIAlive()
    spark.stop()
  }

  private val hdfsPath = SparkStarter.hdfsPath()

  "Dataset" should "have a schema" in {
    val customerDS: Dataset[Customer] = ECommerce.customersDS(10)
    customerDS.printSchema()
  }

  it should "trigger execution only when using an action" in {
    val customerDS: Dataset[Customer] = ECommerce.customersDS(10)
    customerDS.show()
  }

  it should "allow aggregation using Spark SQL DSL to build queries" in {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val orderDS: Dataset[Order] = ECommerce.ordersDS(10, customerId => customerId * 10)

    val result: Dataset[Row] = orderDS
      .groupBy($"customer_id")
      .agg(count($"id").as("order_count"))

    result.printSchema()

    result
      .orderBy($"customer_id")
      .show()
  }

  it should "write to multiple parts instead of a single part" in {
    import spark.implicits._

    val customerDS: Dataset[Customer] = ECommerce.customersDS(10000)
    val orderDS: Dataset[Order] = ECommerce.ordersDS(10000, customerId => customerId)

    val result: Dataset[Row] = customerDS.as("cst")
      .join(orderDS.as("ord"), $"cst.id" === $"ord.customer_id", "left")
      .select(
        $"cst.id".as("customer_id"),
        $"ord.id".as("order_id")
      )

    result.write
      .mode(SaveMode.Overwrite)
      .csv(hdfsPath.resolve("join.csv").toString)

    result.write
      .mode(SaveMode.Overwrite)
      .parquet(hdfsPath.resolve("join.parquet").toString)
  }
}
