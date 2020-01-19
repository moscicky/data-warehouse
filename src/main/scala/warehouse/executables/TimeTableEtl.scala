package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{ETL, TIME_TABLE}

object TimeTableEtl {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("Time table ETL")
      .getOrCreate()

    val path = "src/main/scala/warehouse/data/"

    val etl = new ETL(path)
    etl.D_TIME(spark, TIME_TABLE.name)
  }
}
