package XMLReading

import XMLReading.HWReading.Measurement

import java.sql.{Connection, PreparedStatement}

object DBFunctions extends App{

  /** Creates a table Measurements
   * First column id as integer with autoincrement
   * Columns in accordance with case class
   *
   * @param c a connections with the DB
   */
  def migrateMeasurementsTable(c: Connection): Unit = {
    println("Migrating table for measurements")
    val statement = c.createStatement()

    val sqlCreateMeasurementsTable =
      """
        |DROP TABLE IF EXISTS Measurements;
        |CREATE TABLE IF NOT EXISTS Measurements (
        | Measurement_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        | Station_name TEXT NOT NULL,
        | Stations_European_code TEXT NOT NULL,
        | Component_name TEXT NOT NULL,
        | Component_caption TEXT NOT NULL,
        | Measurement_unit TEXT NOT NULL,
        | Measurement_technique_principle TEXT NOT NULL,
        | Mean TEXT NOT NULL,
        | Median TEXT NOT NULL
        |);
        |""".stripMargin

    statement.executeUpdate(sqlCreateMeasurementsTable)
    statement.close()
  }

  /** Inserting an Measurement object into the created table
   * Taking parameters from single object
   *
   * @param c               connection with the DB
   * @param m the object from which parameters are taken
   */
  def insertMeasurement(c: Connection, m: Measurement): Unit = {

    val insertSql =
      """
        |INSERT INTO measurements (
        |    Station_name,
        |    Stations_European_code,
        |    Component_name,
        |    Component_caption,
        |    Measurement_unit,
        |    Measurement_technique_principle,
        |    Mean,
        |    Median)
        |VALUES (?,?,?,?,?,
        |         ?,?,?);
""".stripMargin

    val preparedStmt: PreparedStatement = c.prepareStatement(insertSql)

    preparedStmt.setString(1, m.stationName)
    preparedStmt.setString(2, m.stationsEuropeanCode)
    preparedStmt.setString(3, m.componentName)
    preparedStmt.setString(4, m.componentCaption)
    preparedStmt.setString(5, m.measurementUnit)
    preparedStmt.setString(6, m.measurementTechniquePrinciple)
    preparedStmt.setString(7, m.mean)
    preparedStmt.setString(8, m.median)
    preparedStmt.execute
    preparedStmt.close()
  }

}
