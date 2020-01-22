sealed trait Table {
  val name: String
}

object Table {
  def allNames(): List[String] = all().map(_.name)

  def all(): List[Table] = List(TIME_TABLE, LOCATION_TABLE, CRIME_TYPE_TABLE,
    AIR_POLLUTION_TYPE_TABLE, CRIME_OUTCOME_TABLE, SOURCE_TABLE, AIR_QUALITY_TABLE, CRIME_TABLE)
}

case object TIME_TABLE extends Table {
  val name = "d_time"
}

case object LOCATION_TABLE extends Table {
  val name = "d_location"
}

case object CRIME_TYPE_TABLE extends Table {
  val name = "d_crime_type"
}

case object AIR_POLLUTION_TYPE_TABLE extends Table {
  val name = "d_air_pollution_type"
}

case object CRIME_OUTCOME_TABLE extends Table {
  val name = "d_crime_outcome_type"
}

case object SOURCE_TABLE extends Table {
  val name = "d_source"
}

case object AIR_QUALITY_TABLE extends Table {
  val name = "f_air_quality"
}

case object CRIME_TABLE extends Table {
  val name = "f_crime"
}

import org.apache.spark.sql.{DataFrame, SparkSession}

class SchemaCreator(spark: SparkSession) {

  def dropSchema(tables: List[String]) = {
    tables.foreach(table => spark.sql(s"DROP TABLE IF EXISTS $table"))
  }

  def createAll(): Unit = {
    dropSchema(Table.allNames())
    Table.all().foreach {
      case t@TIME_TABLE => createTimeTable(t.name)
      case t@LOCATION_TABLE => createLocationTable(t.name)
      case t@CRIME_TYPE_TABLE => createCrimeTypeTable(t.name)
      case t@AIR_POLLUTION_TYPE_TABLE => createAirPollutionTypeTable(t.name)
      case t@SOURCE_TABLE => createSourceTypeTable(t.name)
      case t@CRIME_OUTCOME_TABLE => createCrimeOutcomeTable(t.name)
      case t@AIR_QUALITY_TABLE => createAirQualityTable(t.name)
      case t@CRIME_TABLE => createCrimeTable(t.name)
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

  def createAirPollutionTypeTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
         |id LONG,
         |pollutionType STRING,
         |norm INT) USING hive""".stripMargin)
  }

  def createCrimeOutcomeTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
         |id LONG,
         |outcome STRING) USING hive""".stripMargin)
  }

  def createSourceTypeTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
         |id LONG,
         |source STRING) USING hive""".stripMargin)
  }

  def createAirQualityTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
         |timeId LONG,
         |typeId LONG,
         |normExceeded BOOLEAN) USING hive""".stripMargin)
  }

  def createCrimeTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
         |crimeId STRING,
         |timeId LONG,
         |locationId LONG,
         |crimeTypeId LONG,
         |sourceId LONG,
         |outcomeId LONG) USING hive""".stripMargin)
  }
}
