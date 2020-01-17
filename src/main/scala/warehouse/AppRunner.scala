package warehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AppRunner extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Data warehouse")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR") //żeby było mniej logów

  val path = "src/main/scala/warehouse/data/"

  val airQualityTable = "air_quality"
  val timeTable = "d_time"

  val schemaCreator = new SchemaCreator(spark)
  schemaCreator.createTimeTable(timeTable)

  val etl = new ETL()

  etl.D_TIME(spark, timeTable)

  spark.sql(s"SELECT COUNT (*) FROM $timeTable").collect().toList.foreach(println)
  spark.sql(s"SELECT SUM(month) FROM $timeTable").collect().toList.foreach(println)


  spark.read
    .table(timeTable)
    .as[Time]
    .agg(sum($"year")).as("year_sum")
    .show()
}


