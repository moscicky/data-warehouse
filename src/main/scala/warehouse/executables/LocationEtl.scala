package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{LOCATION_TABLE, ETL}

object LocationEtl {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("LOCATION_TABLE table ETL")
      .enableHiveSupport()
      .getOrCreate()


    //1 argument - ścieżka do plików wejściowych
    val path = args(0)

    //2 argument - czu używac małych wersji plików
    val etl = new ETL(path, args(1).toBoolean)

    etl.D_LOCATION(spark, LOCATION_TABLE.name)
  }
}
