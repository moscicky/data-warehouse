package warehouse

import org.apache.spark.sql.SparkSession

object AppRunner extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Data warehouse")
    .enableHiveSupport()
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

  val airQualityTable = "air_quality"
  val tempAirQualityTable = s"temp_$airQualityTable"

  //tworzenie tabeli hive'owej
  airQuality.createOrReplaceTempView(tempAirQualityTable)
  spark.sql(s"drop table if exists $airQualityTable")
  spark.sql(s"create table $airQualityTable as select * from $tempAirQualityTable")
  spark.sql(s"SELECT COUNT (*) FROM $airQualityTable").collect().toList.foreach(println)

  val schemaCreator = new SchemaCreator(spark)
  schemaCreator.createTimeTable()
  spark.sql("""INSERT INTO d_time VALUES(123, 123)""")
  spark.sql(s"SELECT (*) FROM d_time").collect().toList.foreach(println)
}
