package sparkstarter

import java.nio.file.Paths

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Column
import org.scalatest.{FunSpec, Matchers}

class SparkStarterDataFrameSpec extends FunSpec with Matchers with DataFrameSuiteBase {
  describe("Sample") {
    it("should work") {
      import org.apache.spark.sql.functions._
      import org.apache.spark.sql.types._
      import spark.implicits._

      def group(number: Column): Column = number % 3
      def square(number: Column): Column = number * number

      val groupedSquareSumsDF = (1 to 10).toDF("number")
        .select(group($"number").as("group"), square($"number").cast(LongType).as("square"))
        .groupBy($"group")
        .agg(sum($"square").as("square_sum"))

      val schema = StructType(Seq(
        StructField("group", IntegerType),
        StructField("square_sum", LongType)
      ))

      groupedSquareSumsDF.schema should be(schema)
    }
  }

  describe("JSON file") {
    it("should be readable") {
      val petsCsvPath = Paths.get(getClass.getResource("/pets.json").toURI)
      import spark.implicits._
      import org.apache.spark.sql.types._

      val petSchema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      ))

      val petsDF = spark.read
        .option("multiline", true)
        .schema(petSchema)
        .json(petsCsvPath.toString)

      val expectedPetsDF = spark.createDataFrame(
        Seq(
          Pet(1, "Rex"),
          Pet(2, "Mistigri"),
          Pet(3, "Randolph")
        ).toDF.rdd,
        petSchema
      )

      assertDataFrameEquals(expectedPetsDF, petsDF)
    }
  }
}
