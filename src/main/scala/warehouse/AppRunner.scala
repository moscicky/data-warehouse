package warehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import warehouse.model.{AirPollutionType, AirQuality, CrimeType, Location, OutcomeType, SourceType, Time, Crime}

object AppRunner extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Data warehouse")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR") //żeby było mniej logów

  val path = "src/main/scala/warehouse/data/"

  //zdropowanie kazdej tabeli i stawianie na nowo
  val schemaCreator = new SchemaCreator(spark)
  schemaCreator.dropSchema(Table.allNames())
  schemaCreator.createAll()

  //etl dla kazdej tabli
  val etl = new ETL(path)
  etl.all(spark)

  //przyłady analizy z wykorzystaniem sql
  spark.sql(s"SELECT COUNT (*) FROM ${TIME_TABLE.name}").collect().toList.foreach(println)
  spark.sql(s"SELECT SUM(month) FROM ${TIME_TABLE.name}").collect().toList.foreach(println)

  //przykłady analizy dataset api
  spark.read
    .table(TIME_TABLE.name)
    .as[Time]
    .agg(sum($"year")).as("year_sum")
    .show()

  spark.read
    .table(LOCATION_TABLE.name)
    .as[Location]
    .show(5)

  spark.read
    .table(CRIME_TYPE_TABLE.name)
    .as[CrimeType]
    .show(5)

  spark.read
    .table(AIR_POLLUTION_TYPE_TABLE.name)
    .as[AirPollutionType]
    .show(5)

  spark.read
    .table(CRIME_OUTCOME_TABLE.name)
    .as[OutcomeType]
    .show(5)

  spark.read
    .table(SOURCE_TABLE.name)
    .as[SourceType]
    .show(5)

  spark.read
    .table(TIME_TABLE.name)
    .as[Time]
    .show(5)

  spark.read
    .table(AIR_QUALITY_TABLE.name)
    .as[AirQuality]
   .show(5)

  spark.read
    .table(CRIME_TABLE.name)
    .as[Crime]
    .show(5)

}


