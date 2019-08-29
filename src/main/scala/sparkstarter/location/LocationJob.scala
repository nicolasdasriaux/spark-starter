package sparkstarter.location

import org.apache.spark.sql.{Dataset, SparkSession}

object LocationJob {
  /**
   * Read CSV file containing locations a `Dataset` of [[RawLocation]]
   * @param locationJsonPath Path to JSON file
   * @param spark Spark Session
   * @return Raw Locations
   */
  def loadRawLocations(locationJsonPath: String)(implicit spark: SparkSession): Dataset[RawLocation] = {
    import spark.implicits._
    import org.apache.spark.sql.types._

    /**
     * Describe schema of raw location
     *
     * Hints:
     *
     * - Read through the `src/resources/location/locations.csv` file and take note of its inconsistencies
     * - Use string fields only as data in the actual file is highly inconsistent
     * - Use [[org.apache.spark.sql.types.StructType]]
     * - Use [[scala.collection.Seq]]
     * - Use [[org.apache.spark.sql.types.StructField]]
     * - Use [[org.apache.spark.sql.types.StringType]]
     */
    val locationSchema: StructType = StructType(Seq(
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

    /**
     * Read CSV
     *
     * Hints:
     *
     * - Use [[org.apache.spark.sql.SparkSession.read]]
     * - Use [[org.apache.spark.sql.DataFrameReader.json()]] and look at ScalaFoc for available options
     * - Use [[org.apache.spark.sql.DataFrameReader.option()]]
     * - Use [[org.apache.spark.sql.DataFrameReader.schema()]]
     * - Use [[org.apache.spark.sql.Dataset.as]] to convert `DataFrame` to proper `Dataset`
     */
    val rawLocationsDS: Dataset[RawLocation] = spark.read
      .option("multiline", true)
      .option("mode", "FAILFAST")
      .schema(locationSchema)
      .json(locationJsonPath)
      .as[RawLocation]

    rawLocationsDS
  }

  /**
   * Clean `Dataset` of [[RawLocation]] to a `Dataset` of [[Location]]
   *
   * @param rawLocationsDS Raw Locations
   * @param spark Spark session
   * @return Locations
   */
  def cleanLocations(rawLocationsDS: Dataset[RawLocation])(implicit spark: SparkSession): Dataset[Location] = {
    import spark.implicits._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._

    /**
     * Clean locations
     *
     * Hints:
     * - Observe source structures ([[RawLocation]], [[RawCoordinates]]) and target structures ([[Location]] and [[Coordinates]])
     *   especially names and types of attributes
     * - Use [[org.apache.spark.sql.Dataset.withColumn()]] to add or replace columns
     * - Use [[org.apache.spark.sql.Column.cast()]] to convert as column to proper type
     * - Use [[org.apache.spark.sql.types.LongType]] and [[org.apache.spark.sql.types.DoubleType]]
     * - Use [[org.apache.spark.sql.Column.as()]] to name a column
     * - Use [[org.apache.spark.sql.Dataset.drop()]] to remove  columns
     * - Use [[org.apache.spark.sql.functions.struct()]] to form a structured column for `coordinates`
     * - Use [[org.apache.spark.sql.functions.coalesce()]] to take first non-null column
     * - Use [[org.apache.spark.sql.Dataset.as]] to convert `DataFrame` to proper `Dataset`
     */
    val locationsDS: Dataset[Location] = rawLocationsDS
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
