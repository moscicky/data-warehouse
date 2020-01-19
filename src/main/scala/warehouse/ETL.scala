package warehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import warehouse.model.{AirPollutionType, CrimeType, Location, Time}

class ETL(val path: String) {
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
      csv(s"$path/AirQuality1000.csv").
      cache();

    val air_quality_norms_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/Norm.csv").
      cache();

    val parsed = air_quality_headers_DS.limit(1)
      .select(posexplode(array("_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8")))
      .withColumnRenamed("col", "pollutionType")
    
    parsed.join(air_quality_norms_DS,"pollutionType")
      .withColumnRenamed("pos", "pollutionType", "norm")
      .as[AirPollutionType]
      .write.insertInto(tableName)

  }

  def D_TIME(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._

    val metropolitan_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/MetropolitanPoliceServiceRecords1000.csv").
      cache();

    val london_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/CityofLondonPoliceRecords1000.csv").
      cache();

    val air_quality_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/AirQuality1000.csv").
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
      csv(s"$path/MetropolitanPoliceServiceRecords1000.csv").
      cache();

    val london_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/CityofLondonPoliceRecords1000.csv").
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
      csv(s"$path/MetropolitanPoliceServiceRecords1000.csv").
      cache();

    val london_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/CityofLondonPoliceRecords1000.csv").
      cache();


    london_crime_records_DS.union(metropolitan_crime_records_DS).dropDuplicates("Crime type")
      .withColumn("id", monotonically_increasing_id)
      .select("id", "Crime type")
      .withColumnRenamed("Crime type", "crimeType")
      .as[CrimeType]
      .write.insertInto(tableName)
  }

  def D_OUTCOME_TYPE(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._

    val metropolitan_crime_outcomes_DS = spark.read.format("org.apache.spark.csv").
      option("header", false).option("inferSchema", true).
      csv(s"$path/MetropolitanPoliceServiceOutcomes1000.txt").
      cache();

    val london_crime_outcomes_DS = spark.read.format("org.apache.spark.csv").
      option("header", false).option("inferSchema", true).
      csv(s"$path/CityofLondonPoliceOutcomes1000.txt").
      cache();


    val step1 = london_crime_outcomes_DS
      .union(metropolitan_crime_outcomes_DS).
      withColumn("tmp", split($"_c0", "\\:")).select(
      $"tmp".getItem(0).as("col1"),
      $"tmp".getItem(1).as("col2")
    )
      .select( "col2")
      .dropDuplicates("col2")
      .withColumn("id", monotonically_increasing_id)

    step1.withColumn("col2", trim(step1("col2")))
      .select("id","col2")
      .collect().foreach(x => println(x))
  }

}
