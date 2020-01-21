package warehouse

sealed trait Table {
  val name: String
}

object Table {
  def allNames(): List[String] = all().map(_.name)

  def all(): List[Table] = List(TIME_TABLE, LOCATION_TABLE, CRIME_TYPE_TABLE,
    AIR_POLLUTION_TYPE_TABLE, CRIME_OUTCOME_TABLE, SOURCE_TABLE, AIR_QUALITY_TABLE, CRIME_TABLE)
}

case object TIME_TABLE extends Table {
  val name = "d_time"
}

case object LOCATION_TABLE extends Table {
  val name = "d_location"
}

case object CRIME_TYPE_TABLE extends Table {
  val name = "d_crime_type"
}

case object AIR_POLLUTION_TYPE_TABLE extends Table {
  val name = "d_air_pollution_type"
}

case object CRIME_OUTCOME_TABLE extends Table {
  val name = "d_crime_outcome_type"
}

case object SOURCE_TABLE extends Table {
  val name = "d_source"
}

case object AIR_QUALITY_TABLE extends Table {
  val name = "f_air_quality"
}

case object CRIME_TABLE extends Table {
  val name = "f_crime"
}