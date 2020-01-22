package warehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import warehouse.AppRunner.spark
import warehouse.model.{AirPollutionType, AirQuality, CrimeType, Location, OutcomeType, SourceType, Time, Crime}

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
      case t@CRIME_OUTCOME_TABLE => D_OUTCOME_TYPE(spark, t.name)
      case t@SOURCE_TABLE => D_SOURCE_TYPE(spark, t.name)
      case t@AIR_QUALITY_TABLE => F_AIR_QUALITY(spark, t.name)
      case t@CRIME_TABLE => F_CRIME(spark, t.name)
    }
  }

  def D_AIR_POLLUTION_TYPE(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._

    val air_quality_headers_DS = spark.read.format("org.apache.spark.csv").
      option("header", false).option("inferSchema", true).
      csv(s"$path/$airQualityFile").
      cache();

    val air_quality_norms_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/Norm.csv").
      cache();

    val parsed = air_quality_headers_DS.limit(1)
      .select(posexplode(array( "_c3",  "_c5", "_c6"))) //dla uproszczenia wybieram tylko te, które są nam potrzebne tak żeby miały przewidywalne id od 0 do 2
      .withColumnRenamed("col", "pollutionType")

    parsed.join(air_quality_norms_DS,"pollutionType")
      .withColumnRenamed("pos", "id")
      .select("id", "pollutionType", "norm")
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

  def D_OUTCOME_TYPE(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._

    val metropolitan_crime_outcomes_DS = spark.read.format("org.apache.spark.csv").
      option("header", false).option("inferSchema", true).
      csv(s"$path/$metropolitanPoliceOutcomesFile").
      cache();

    val london_crime_outcomes_DS = spark.read.format("org.apache.spark.csv").
      option("header", false).option("inferSchema", true).
      csv(s"$path/$londonPoliceOutcomesFile").
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

    step1.withColumn("outcome", trim(step1("col2")))
      .select("id","outcome")
      .as[OutcomeType]
      .write.insertInto(tableName)
  }

  def D_SOURCE_TYPE(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._

    val source_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/Source.csv").
      cache();

    source_DS.as[SourceType].write.insertInto(tableName)
  }

  def F_AIR_QUALITY(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._

    val air_quality_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/$airQualityFile").
      cache();

    val air_quality_norms_DS = spark.read
      .table(AIR_POLLUTION_TYPE_TABLE.name)
      .as[AirPollutionType]

    val step1 = air_quality_DS.groupBy("Month (text)")
      .agg(avg("London Mean Roadside Nitric Oxide (ug/m3)") as "London Mean Roadside Nitric Oxide (ug/m3)",
        avg("London Mean Roadside Nitrogen Dioxide (ug/m3)") as "London Mean Roadside Nitrogen Dioxide (ug/m3)",
        avg("London Mean Roadside Ozone (ug/m3)") as "London Mean Roadside Ozone (ug/m3)",
        avg("London Mean Roadside PM10 Particulate (ug/m3)") as "London Mean Roadside PM10 Particulate (ug/m3)",
        avg("London Mean Roadside Sulphur Dioxide (ug/m3)") as "London Mean Roadside Sulphur Dioxide (ug/m3)" // gdy biorę kolumnę London Mean Roadside PM2.5 Particulate (ug/m3) wywala mi dziwny błąd :O
      )
      .select($"Month (text)", posexplode(array("London Mean Roadside Nitrogen Dioxide (ug/m3)", "London Mean Roadside Ozone (ug/m3)", "London Mean Roadside PM10 Particulate (ug/m3)")))
      .withColumnRenamed("col", "pollutionValue")
      .withColumnRenamed("pos", "pollutionId")
      .join(air_quality_norms_DS, $"pollutionId" === $"id")
      .withColumn("normExceeded", when($"pollutionValue" > $"norm", true).otherwise(false))
      .select(month(col("Month (text)")) as "monthNum", year(col("Month (text)")) as "yearNum", col("pollutionId") as "typeId", col("normExceeded"));

    val time_DS = spark.read
      .table(TIME_TABLE.name)
      .as[Time]

    val step2 = step1.join(time_DS, ($"month" === $"monthNum" && $"year" === $"yearNum"))
      .withColumnRenamed("id", "timeId")
      .select("timeId", "typeId", "normExceeded")
      .as[AirQuality]
      .write
      .insertInto(tableName)

  }

  def F_CRIME(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val slice = udf((array : String, from : Int, to : Int) => array.slice(from,to))

    val metropolitan_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/$metropolitanPoliceRecordsFile").
      cache()
      .withColumn("sourceId", lit(1));

    val london_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/$londonPoliceRecordsFile").
      cache()
      .withColumn("sourceId", lit(0));

    val metropolitan_crime_outcomes_DS = spark.read.format("org.apache.spark.csv").
      option("header", false).option("inferSchema", true).
      csv(s"$path/$metropolitanPoliceOutcomesFile").
      cache();

    val london_crime_outcomes_DS = spark.read.format("org.apache.spark.csv").
      option("header", false).option("inferSchema", true).
      csv(s"$path/$londonPoliceOutcomesFile").
      cache();

    val crime_outcomes_parsed = metropolitan_crime_outcomes_DS
        .union(london_crime_outcomes_DS)
        .withColumn("tmp", split($"_c0", "\\:")).select(
          $"tmp".getItem(0).as("left"),
          $"tmp".getItem(1).as("outcome")
        )
      .withColumn("id", col("left").substr(18, 64))
      .withColumn("date", col("left").substr(96, 7))
      .withColumn("outcome", col("outcome").substr(2, 1000))
      //widziałem, że outcome w tabeli zaczynały się od " ", więc usunąłem pierwszy znak (?)
      .drop("left")
      .select(month(col("date")) as "monthNum", year(col("date")) as "yearNum",
        $"id" as "crimeId", $"outcome")

    val records_parsed = metropolitan_crime_records_DS
      .union(london_crime_records_DS)
      .filter($"Crime ID".isNotNull)

    val records_joined = crime_outcomes_parsed
      .join(records_parsed, $"Crime ID" === $"crimeId")
      .drop("month")

    val time_DS = spark.read
      .table(TIME_TABLE.name)
      .as[Time]

    val location_DS = spark.read
      .table(LOCATION_TABLE.name)
      .as[Location]

    val crime_type_DS = spark.read
      .table(CRIME_TYPE_TABLE.name)
      .as[CrimeType]

    val outcome_type_DS = spark.read
      .table(CRIME_OUTCOME_TABLE.name)
      .as[OutcomeType]

    val mega_join = records_joined
      .join(time_DS, ($"yearNum" === $"year" && $"monthNum" === $"month"))
      .withColumnRenamed("id", "timeId")
      .join(location_DS, ($"lsoaCode" === $"LSOA code" && $"lsoaName" === $"LSOA name"))
      .withColumnRenamed("id", "locationId")
      .withColumnRenamed("outcome", "crimeOutcome")
      .join(outcome_type_DS, ($"outcome" === $"crimeOutcome"))
      .withColumnRenamed("id", "outcomeId")
      .join(crime_type_DS, ($"Crime type" === $"crimeType"))
      .withColumnRenamed("id", "crimeTypeId")
      .groupBy("timeId", "locationId", "crimeTypeId", "sourceId", "outcomeId")
      .count()
      .select("count", "timeId", "locationId", "crimeTypeId", "sourceId", "outcomeId")
      .as[Crime]
      .write
      .insertInto(tableName)

  }

}
