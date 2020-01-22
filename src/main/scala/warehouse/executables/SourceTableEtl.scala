package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{ETL, SOURCE_TABLE}

object SourceTableEtl {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("SOURCE_TABLE table ETL")
      .enableHiveSupport()
      .getOrCreate()


    //1 argument - ścieżka do plików wejściowych
    val path = args(0)

    //2 argument - czu używac małych wersji plików
    val etl = new ETL(path, args(1).toBoolean)

    etl.D_SOURCE_TYPE(spark, SOURCE_TABLE.name)
  }
}
