package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{ETL, AIR_QUALITY_TABLE}

object AirQualityTableEtl {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("AIR_QUALITY_TABLE table ETL")
      .enableHiveSupport()
      .getOrCreate()


    //1 argument - ścieżka do plików wejściowych
    val path = args(0)

    //2 argument - czu używac małych wersji plików
    val etl = new ETL(path, args(1).toBoolean)

    etl.F_AIR_QUALITY(spark, AIR_QUALITY_TABLE.name)
  }
}
