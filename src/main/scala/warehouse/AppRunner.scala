package warehouse

import org.apache.spark.sql.SparkSession

object AppRunner extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Data warehouse")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR") //żeby było mniej logów

  val path = "src/main/scala/warehouse/data/"

  //przykład jak zaczytać dane
  val airQuality = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(s"$path/AirQuality1000.csv")

  airQuality.printSchema()

  println(airQuality.count())
}
