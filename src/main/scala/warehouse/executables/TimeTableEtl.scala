package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{ETL, TIME_TABLE}

object TimeTableEtl {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("Time table ETL")
      .enableHiveSupport()
      .getOrCreate()


    //1 argument - ścieżka do plików wejściowych
    val path = args(0)

    //2 argument - czu używac małych wersji plików
    val etl = new ETL(path, args(1).toBoolean)

    etl.D_TIME(spark, TIME_TABLE.name)
  }
}
