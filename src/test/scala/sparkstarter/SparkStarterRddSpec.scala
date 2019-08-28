package sparkstarter

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FunSpec, Matchers}

class SparkStarterRddSpec extends FunSpec with Matchers with SharedSparkContext with RDDComparisons {
  describe("Sample") {
    it("should work") {
      val numbersRDD = sc.parallelize(1 to 10)
        .filter(i => i % 3 == 0)
        .map(i => s"Number $i")

      val expectedNumbersRDD = sc.parallelize(
        Seq(
          "Number 3",
          "Number 6",
          "Number 9"
        )
      )

      assertRDDEquals(expectedNumbersRDD, numbersRDD)
    }
  }
}
