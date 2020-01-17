package warehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col, monotonically_increasing_id, month, posexplode, year}
import org.apache.spark.sql.functions._

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

  def D_AIR_POLLUTION_TYPE(): Unit ={
    val path = "src/main/scala/warehouse/data/"

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Data warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") //żeby było mniej logów


    val air_quality_headers_DS = spark.read.format("org.apache.spark.csv").
      option("header", false).option("inferSchema", true).
      csv(s"$path/AirQuality1000.csv").
      cache();


    air_quality_headers_DS.limit(1).select(posexplode(array("_c2","_c3","_c4","_c5","_c6","_c7","_c8"))).foreach(x => println(x))

  }

  def D_TIME(): Unit ={
    val path = "src/main/scala/warehouse/data/"

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Data warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") //żeby było mniej logów

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



    london_crime_records_DS.union(metropolitan_crime_records_DS).select("Month").union(air_quality_DS.select("Month (text)")).dropDuplicates("Month").select(month(col("month")) as "month" ,year(col("month")) as "year").withColumn("id", monotonically_increasing_id).select("id","month","year").collect().foreach(x => println(x))

  }

  def D_LOCATION(): Unit ={
    val path = "src/main/scala/warehouse/data/"

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Data warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") //żeby było mniej logów

    val metropolitan_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/MetropolitanPoliceServiceRecords1000.csv").
      cache();

    val london_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/CityofLondonPoliceRecords1000.csv").
      cache();

    london_crime_records_DS.union(metropolitan_crime_records_DS).dropDuplicates("Location","LSOA code","LSOA name").withColumn("id", monotonically_increasing_id).select("id","LSOA code","LSOA name","Location").collect().foreach(x => println(x))

  }

  def D_CRIME_TYPE(): Unit ={
    val path = "src/main/scala/warehouse/data/"

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Data warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") //żeby było mniej logów

    val metropolitan_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/MetropolitanPoliceServiceRecords1000.csv").
      cache();

    val london_crime_records_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).option("inferSchema", true).
      csv(s"$path/CityofLondonPoliceRecords1000.csv").
      cache();


    london_crime_records_DS.union(metropolitan_crime_records_DS).dropDuplicates("Crime type").withColumn("id", monotonically_increasing_id).select("id","Crime type").collect().foreach(x => println(x))
  }

  D_AIR_POLLUTION_TYPE()
  D_TIME()
  D_LOCATION()
  D_CRIME_TYPE()
}
