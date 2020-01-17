package warehouse

import org.apache.spark.sql.{DataFrame, SparkSession}

class SchemaCreator(spark: SparkSession) {
  def createTimeTable(tableName: String): DataFrame = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(
      s"""CREATE TABLE $tableName(
        |id LONG,
        |month INT,
        |year INT) USING hive""".stripMargin)
  }
}