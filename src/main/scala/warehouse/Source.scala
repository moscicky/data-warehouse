package warehouse

sealed trait Source {
  val fileName: String
  def small(): String = {
    val tmp = name.split('.')
    s"${tmp(0)}1000.${tmp(1)}"
  }

  def name: String
}

case object AirQualityFile extends Source {
  val fileName = "AirQuality"

  override def name: String = s"$fileName.csv"
}

case object LondonPoliceOutcomesFile extends Source {
  val fileName = "CityofLondonPoliceOutcomes"
  override def name = s"$fileName.txt"
}

case object LondonPoliceRecordsFile extends Source {
  val fileName = "CityofLondonPoliceRecords"
  override def name = s"$fileName.csv"
}

case object MetropolitanPoliceRecordsFile extends Source {
  val fileName = "MetropolitanPoliceServiceRecords"
  override def name = s"$fileName.csv"
}

case object MetropolitanPoliceOutcomesFile extends Source {
  val fileName = "MetropolitanPoliceServiceOutcomes"
  override def name = s"$fileName.txt"
}


