package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{CRIME_OUTCOME_TABLE, ETL}

object OutcomeTypeEtl {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("CRIME_OUTCOME_TABLE table ETL")
      .enableHiveSupport()
      .getOrCreate()


    //1 argument - ścieżka do plików wejściowych
    val path = args(0)

    //2 argument - czu używac małych wersji plików
    val etl = new ETL(path, args(1).toBoolean)

    etl.D_OUTCOME_TYPE(spark, CRIME_OUTCOME_TABLE.name)
  }
}
