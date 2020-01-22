package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{ETL, AIR_POLLUTION_TYPE_TABLE}

object AirPollutionTypeEtl {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("AIR_POLLUTION_TYPE_TABLE table ETL")
      .enableHiveSupport()
      .getOrCreate()


    //1 argument - ścieżka do plików wejściowych
    val path = args(0)

    //2 argument - czu używac małych wersji plików
    val etl = new ETL(path, args(1).toBoolean)

    etl.D_AIR_POLLUTION_TYPE(spark, AIR_POLLUTION_TYPE_TABLE.name)
  }
}
