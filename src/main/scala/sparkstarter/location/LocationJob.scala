package sparkstarter.location

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}

object LocationJob {
  def loadRawLocations(locationJsonPath: String)(implicit spark: SparkSession): Dataset[RawLocation] = {
    import spark.implicits._

    val locationSchema = StructType(Seq(
      StructField("id", StringType),
      StructField("name", StringType),
      StructField("address", StringType),

      StructField("coordinates",
        StructType(Seq(
          StructField("latitude", StringType),
          StructField("longitude", StringType)
        ))
      ),

      StructField("latitude", StringType),
      StructField("longitude", StringType),
      StructField("position", StringType)
    ))

    val rawLocationsDS = spark.read
      .option("multiline", true)
      .option("mode", "FAILFAST")
      .schema(locationSchema)
      .json(locationJsonPath)
      .as[RawLocation]

    rawLocationsDS
  }

  def cleanLocations(rawLocationsDS: Dataset[RawLocation])(implicit spark: SparkSession): Dataset[Location] = {
    import spark.implicits._

    val locationsDS = rawLocationsDS
      .withColumn("id", $"id".cast(LongType))
      .withColumn("coordinates",
        struct(
          coalesce($"latitude", $"coordinates.latitude").cast(DoubleType).as("latitude"),
          coalesce($"longitude", $"coordinates.longitude").cast(DoubleType).as("longitude")
        )
      )
      .drop("latitude", "longitude")
      .as[Location]

    locationsDS
  }
}
