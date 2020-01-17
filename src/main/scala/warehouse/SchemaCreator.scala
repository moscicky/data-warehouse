package warehouse

import org.apache.spark.sql.{DataFrame, SparkSession}

class SchemaCreator(spark: SparkSession) {

  def dropSchema(tables: List[String]) = {
    tables.foreach(table => spark.sql(s"DROP TABLE IF EXISTS $table"))
  }

  def createAll(): Unit = {
    Table.all().foreach {
      case t@TIME_TABLE => createTimeTable(t.name)
      case t@LOCATION_TABLE => createLocationTable(t.name)
      case t@CRIME_TYPE_TABLE => createCrimeTypeTable(t.name)
      case t@AIR_POLLUTION_TYPE_TABLE => createAirPollutionTypeTable(t.name)
    }
  }

  def createTimeTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
         |id LONG,
         |month INT,
         |year INT) USING hive""".stripMargin)
  }

  def createLocationTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
         |id LONG,
         |lsoaCode STRING,
         |lsoaName STRING,
         |locationType STRING) USING hive""".stripMargin)
  }

  def createCrimeTypeTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
         |id LONG,
         |crimeType STRING) USING hive""".stripMargin)
  }

  //TODO: add norm
  def createAirPollutionTypeTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
         |id LONG,
         |pollutionType STRING) USING hive""".stripMargin)
  }
}