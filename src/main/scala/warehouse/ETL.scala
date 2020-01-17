package warehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class ETL {

  val path = "src/main/scala/warehouse/data/"


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

    air_quality_headers_DS.printSchema()

    air_quality_headers_DS.limit(1).select(posexplode(array("_c2","_c3","_c4","_c5","_c6","_c7","_c8"))).foreach(x => println(x))

  }

  def D_TIME(spark: SparkSession, tableName: String): Unit ={
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
      .select(month(col("month")) as "month" ,year(col("month")) as "year")
      .withColumn("id", monotonically_increasing_id).select("id","month","year")
      .as[Time]
      .write.insertInto(tableName)
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

    london_crime_records_DS.union(metropolitan_crime_records_DS)
      .dropDuplicates("Location","LSOA code","LSOA name")
      .withColumn("id", monotonically_increasing_id)
      .select("id","LSOA code","LSOA name","Location")
      .collect().foreach(x => println(x))

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

}
