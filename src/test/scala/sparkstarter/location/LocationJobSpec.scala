package sparkstarter.location

import java.nio.file.Paths

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}

class LocationJobSpec extends FunSpec with Matchers with DatasetSuiteBase {
  describe("Location Job") {
    it("should launch program") {
      val locationJsonPath = Paths.get(getClass.getResource("/location/regular/locations.json").toURI)
      implicit val spark: SparkSession = this.spark
      import spark.implicits._

      val expectedRawLocationsDS = Seq(
        RawLocation(
          id = "1",
          name = "Paul Hochon",
          address = "5 rue du chat",
          coordinates = RawCoordinates(latitude = "1.1", longitude = "1.2"),
          latitude = null,
          longitude = null,
          position = null
        ),
        RawLocation(
          id = "2",
          name = "Pierre Affeu",
          address = "9 avenue du rat",
          coordinates = null,
          latitude = "2.1",
          longitude = "2.2",
          position = "France"
        )
      ).toDS

      val rawLocationsDS = LocationJob.loadRawLocations(locationJsonPath.toString)
      expectedRawLocationsDS.show()
      rawLocationsDS.show()

      assertDatasetEquals(expectedRawLocationsDS, rawLocationsDS)
    }

    it("should clean coordinates") {
      implicit val spark: SparkSession = this.spark
      import spark.implicits._

      val rawLocationsDS = Seq(
        RawLocation(
          id = "1",
          name = "Paul Hochon",
          address = "5 rue du chat",
          coordinates = RawCoordinates(latitude = "1.1", longitude = "1.2"),
          latitude = null,
          longitude = null,
          position = null
        ),
        RawLocation(
          id = "2",
          name = "Pierre Affeu",
          address = "9 avenue du rat",
          coordinates = null,
          latitude = "2.1",
          longitude = "2.2",
          position = "France"
        )
      ).toDS

      val expectedLocationsDS = Seq(
        Location(
          id = 1,
          name = "Paul Hochon",
          address = "5 rue du chat",
          coordinates = Coordinates(latitude = 1.1, longitude = 1.2),
          position = null
        ),
        Location(
          id = 2,
          name = "Pierre Affeu",
          address = "9 avenue du rat",
          coordinates = Coordinates(latitude = 2.1, longitude = 2.2),
          position = "France"
        )
      ).toDS

      val locationsDS = LocationJob.cleanLocations(rawLocationsDS)
      assertDatasetEquals(expectedLocationsDS, locationsDS)
    }
  }
}
