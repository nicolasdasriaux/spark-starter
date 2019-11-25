package sparkstarter

import com.holdenkarau.spark.testing.RDDComparisons
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import sparkstarter.ecommerce.{Customer, ECommerce, Order}

class SparkStarterRddSpec extends FlatSpec with Matchers with BeforeAndAfterAll with RDDComparisons {
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("RDD")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 20) // Parallelism when shuffling in Spark SQL
    .getOrCreate()

  implicit val sc: SparkContext = spark.sparkContext

  override def afterAll() {
    SparkStarter.keepSparkUIAlive()
    spark.stop()
  }

  "RDD" should "trigger execution only when using an action" in {
    val numbersRDD = sc.parallelize(1 to 10)
      .filter(i => i % 3 == 0)
      .map(i => s"Number $i")

    println(numbersRDD.collect().toList)
  }

  it should "allow aggregation by key" in {
    val orderRDD: RDD[Order] = ECommerce.orderRDD(10, customerId => customerId)

    val customerIdAndOneRDD: RDD[(Long, Int)] = orderRDD.map(order => (order.customer_id, 1))
    val byCustomerIdOrderCountRDD: RDD[(Long, Int)] = customerIdAndOneRDD.reduceByKey(_ + _)

    print(byCustomerIdOrderCountRDD.collect().toList)
  }

  it should "allow joins using pair RDDs" in {
    val customerRDD: RDD[Customer] = ECommerce.customersRDD(10)
    val orderRDD: RDD[Order] = ECommerce.orderRDD(10, customerId => customerId)

    val customerIdAndCustomerRDD: RDD[(Long, Customer)] = customerRDD.map(customer => (customer.id, customer))
    val customerIdAndOrderRDD: RDD[(Long, Order)] = orderRDD.map(order => (order.customer_id, order))
    val joinRDD: RDD[(Long, (Customer, Option[Order]))] = customerIdAndCustomerRDD
      .leftOuterJoin(customerIdAndOrderRDD)

    println(joinRDD.collect().toList)
  }
}
