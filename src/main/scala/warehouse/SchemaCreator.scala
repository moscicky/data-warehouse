package warehouse

import org.apache.spark.sql.{DataFrame, SparkSession}

class SchemaCreator(spark: SparkSession) {
  def createTimeTable(): DataFrame = {
    spark.sql(
      """CREATE TABLE IF NOT EXISTS d_time(
        |month INT,
        |year INT) USING hive""".stripMargin)
  }

}
