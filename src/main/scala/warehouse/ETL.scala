package warehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import warehouse.model.{AirPollutionType, CrimeType, Location, Time}

class ETL(val path: String, val small: Boolean = true) {
  private val airQualityFile = if (small) AirQualityFile.small() else AirQualityFile.name
  private val metropolitanPoliceOutcomesFile =
    if (small) MetropolitanPoliceOutcomesFile.small() else MetropolitanPoliceOutcomesFile.name
  private val metropolitanPoliceRecordsFile =
    if (small) MetropolitanPoliceRecordsFile.small() else MetropolitanPoliceRecordsFile.name
  private val londonPoliceOutcomesFile =
    if (small) LondonPoliceOutcomesFile.small() else LondonPoliceOutcomesFile.name
  private val londonPoliceRecordsFile =
    if (small) LondonPoliceRecordsFile.small() else LondonPoliceRecordsFile.name

  def all(spark: SparkSession): Unit = {
    Table.all().foreach {
      case t@TIME_TABLE => D_TIME(spark, t.name)
      case t@LOCATION_TABLE => D_LOCATION(spark, t.name)
      case t@CRIME_TYPE_TABLE => D_CRIME_TYPE(spark, t.name)
      case t@AIR_POLLUTION_TYPE_TABLE => D_AIR_POLLUTION_TYPE(spark, t.name)
    }
  }

  def D_AIR_POLLUTION_TYPE(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._

    val air_quality_headers_DS = spark.read.format("org.apache.spark.csv").
      option("header", false).option("inferSchema", true).
      csv(s"$path/$airQualityFile").
      cache();

    air_quality_headers_DS.limit(1)
      .select(posexplode(array("_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8")))
      .withColumnRenamed("col", "pollutionType")
      .withColumnRenamed("pos", "id")
      .as[AirPollutionType]
      .write.insertInto(tableName)

  }

  def D_TIME(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._

    val metropolitan_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/$metropolitanPoliceRecordsFile").
      cache();

    val london_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/$londonPoliceRecordsFile").
      cache();

    val air_quality_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/$airQualityFile").
      cache();

    london_crime_records_DS.union(metropolitan_crime_records_DS)
      .select("Month").union(air_quality_DS.select("Month (text)")).dropDuplicates("Month")
      .select(month(col("month")) as "month", year(col("month")) as "year")
      .withColumn("id", monotonically_increasing_id).select("id", "month", "year")
      .as[Time]
      .write.insertInto(tableName)
  }

  def D_LOCATION(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._

    val metropolitan_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/$metropolitanPoliceRecordsFile").
      cache();

    val london_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/$londonPoliceRecordsFile").
      cache();

    london_crime_records_DS.union(metropolitan_crime_records_DS)
      .dropDuplicates("Location", "LSOA code", "LSOA name")
      .withColumn("id", monotonically_increasing_id)
      .select("id", "LSOA code", "LSOA name", "Location")
      .withColumnRenamed("LSOA code", "lsoaCode")
      .withColumnRenamed("LSOA name", "lsoaName")
      .withColumnRenamed("Location", "locationType")
      .as[Location]
      .write.insertInto(tableName)
  }

  def D_CRIME_TYPE(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._

    val metropolitan_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/$metropolitanPoliceRecordsFile").
      cache();

    val london_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/$londonPoliceRecordsFile").
      cache();


    london_crime_records_DS.union(metropolitan_crime_records_DS).dropDuplicates("Crime type")
      .withColumn("id", monotonically_increasing_id)
      .select("id", "Crime type")
      .withColumnRenamed("Crime type", "crimeType")
      .as[CrimeType]
      .write.insertInto(tableName)
  }
}
