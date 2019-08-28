package sparkstarter

import java.nio.file.Paths

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.scalatest.{FunSpec, Matchers}

class SparkStarterDatasetSpec extends FunSpec with Matchers with DatasetSuiteBase {
  describe("Sample") {
    it("should work") {
      import spark.implicits._

      val petsDS = Seq(
        Pet(1, "Rex"),
        Pet(2, "Mistigri"),
        Pet(3, "Randolph")
      ).toDS

      val idsDF = petsDS.select($"id")
      val expectedIdsDF = Seq(1, 2, 3).toDF("id")

      assertDataFrameEquals(expectedIdsDF, idsDF)
    }
  }

  describe("CSV") {
    it("should be readable") {
      val petsCsvPath = Paths.get(getClass.getResource("/pets.csv").toURI)
      import spark.implicits._

      val petSchema = ScalaReflection.schemaFor[Pet].dataType.asInstanceOf[StructType]

      val petsDS = spark.read
        .option("header", true)
        .schema(petSchema)
        .csv(petsCsvPath.toString)
        .as[Pet]

      val expectedPetsDS = Seq(
        Pet(1, "Rex"),
        Pet(2, "Mistigri"),
        Pet(3, "Randolph")
      ).toDS

      assertDatasetEquals(expectedPetsDS, petsDS)
    }
  }

  describe("JSON Lines file") {
    it("should be readable") {
      val petsCsvPath = Paths.get(getClass.getResource("/pets.jsonl").toURI)
      import spark.implicits._

      val petSchema = ScalaReflection.schemaFor[Pet].dataType.asInstanceOf[StructType]

      val petsDS = spark.read
        .schema(petSchema)
        .json(petsCsvPath.toString)
        .as[Pet]

      val expectedPetsDS = Seq(
        Pet(1, "Rex"),
        Pet(2, "Mistigri"),
        Pet(3, "Randolph")
      ).toDS

      assertDatasetEquals(expectedPetsDS, petsDS)
    }
  }
}
