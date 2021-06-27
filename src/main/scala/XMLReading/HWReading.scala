package XMLReading

import java.sql.DriverManager
import scala.xml.XML

object HWReading extends App {

  val metaDataPath = "src/resources/IS_meta.xml"
  val metaDataXML = XML.loadFile(metaDataPath)
  val station = metaDataXML \\ "station_info"
  val stationFull = metaDataXML \\ "station"
  val measurements = metaDataXML \\ "measurement_configuration"


  //what SatationData object will contain
  case class StationData(europeanCode: String,
                         localCode: String,
                         name: String,
                         description: String,
                         nutsLevel0: String,
                         nutsLevel1: String,
                         nutsLevel2: String,
                         nutsLevel3: String,
                         countryCode: String,
                         unitCode: String,
                         unitName: String,
                         startDate: String,
                         latitudeDec: String,
                         longitudeDec: String,
                         latitudeDms: String,
                         longitudeDms: String,
                         altitude: String,
                         stationType: String,
                         areaType: String,
                         zoneCharacteristic: String,
                         mainEmissionSource: String,
                         representArea: String,
                         city: String,
                         population: String,
                         StreetName: String,
                         streetType: String,
                         noOfVehicles: String,
                         lorryPercentage: String,
                         trafficSpeed: String,
                         streetWidth: String,
                         monObject: String,
                         meteoPram: String)

  //function, that will filter out stations from the XML
  def fromXMLToStationData(node: scala.xml.Node): StationData = {
    StationData(
      (node \ "station_european_code").text, //europeanCode,
      (node \ "station_local_code").text, //   localCode,
      (node \ "station_name").text, // name,
      (node \ "station_description").text, //description,
      (node \ "station_nuts_level0").text, // nutsLevel0,
      (node \ "station_nuts_level1").text, // nutsLevel1,
      (node \ "station_nuts_level2").text, // nutsLevel2,
      (node \ "station_nuts_level3").text, //    nutsLevel3,
      (node \ "sabe_country_code").text, // countryCode,
      (node \ "sabe_unit_code").text, // unitCode,
      (node \ "sabe_unit_name").text, // unitName,
      (node \ "station_start_date").text, //startDate,
      (node \ "station_latitude_decimal_degrees").text, //latitudeDec,
      (node \ "station_longitude_decimal_degrees").text, // longitudeDec,
      (node \ "station_latitude_dms").text, //  latitudeDms,
      (node \ "station_longitude_dms").text, //longitudeDms,
      (node \ "station_altitude").text, //  altitude,
      (node \ "type_of_station").text, // stationType,
      (node \ "station_type_of_area").text, //areaType,
      (node \ "station_characteristic_of_zone").text, //zoneCharacteristic,
      (node \ "main_emission_source").text, //mainEmissionSource
      (node \ "station_area_of_representativeness").text, //representArea,
      (node \ "station_city").text, //city,
      (node \ "population").text, //population,
      (node \ "street_name").text, //StreetName,
      (node \ "street_type").text, //streetType,
      (node \ "number_of_vehicles").text, //noOfVehicles,
      (node \ "station_lorry_percentage").text, // lorryPercentage,
      (node \ "station_traffic_speed").text, //trafficSpeed,
      (node \ "station_street_width").text, //streetWidth,
      (node \ "monitoring_obj").text, //monObject
      (node \ "meteorological_parameter").text //meteoPram)
    )
  }

  //make a seq of objects out of nodeSeq
  println(s"We have ${station.length} stations")
  println("\nStation Data:")
  val statDataSeq = station.map(el => fromXMLToStationData(el))
  statDataSeq.foreach(println)

  //save StationData as Json
  implicit val stationsDataRW = upickle.default.macroRW[StationData]
  implicit val measurementsRW = upickle.default.macroRW[Measurement]

  val singleJson = upickle.default.write(statDataSeq(0), indent = 4)
  val filesJson = upickle.default.write(statDataSeq, indent = 4)
  val saveJsonPath = "./src/resources/jsonFiles/stations_Iceland_meta.json"

  //used method from created utilities
  def saveString(text: String, destPath: String): Unit = {
    import java.io.{File, PrintWriter}
    val pw = new PrintWriter(new File(destPath))
    pw.write(text)
    pw.close()
  }

  saveString(filesJson, saveJsonPath)

  //what will the measurement object contain
  case class Measurement(
                        stationName: String,
                        stationsEuropeanCode: String,
                        componentName: String,
                        componentCaption: String,
                        measurementUnit: String,
                        measurementTechniquePrinciple: String,
                        mean: String,
                        median: String,
                        )

  //function that helps filter measurements out of XML
  def fromXMLtoMeasurement(node: scala.xml.Node): Measurement = {
    val generalFields = node \\ "measurement_info"
    val statisticalFields = node \\ "statistics" \ "statistics_average_group" \ "statistic_set" \ "statistic_result"

    Measurement(
      (node \ "station_info" \ "station_name").text,
      (node \ "station_info" \ "station_european_code").text,
      (generalFields \ "component_name").text,
      (generalFields \ "component_caption").text,
      (generalFields \ "measurement_unit").text,
      (generalFields \ "measurement_technique_principle").text,
      parseMeanNumber(statisticalFields.filter(n => n \ "statistic_shortname" exists (_.text == "Mean")).text),
      parseMedianNumber(statisticalFields.filter(n => n \ "statistic_shortname" exists (_.text == "P50")).text))

  }

  //helps parsing mean value from results
  def parseMeanNumber (txt:String): String= {
    val meanNumberRegex = raw"(\d+[.]\d{3})".r
    val mean = meanNumberRegex.findFirstIn(txt)
      .getOrElse("No mean")
    mean
  }

  //helps parsing median value from all results
  def parseMedianNumber (txt:String): String= {
    val medianNumberRegex = raw"(\d+[.]\d{3})".r
    val median = medianNumberRegex.findFirstIn(txt)
      .getOrElse("No median")
    median
  }

  //get sew of measurements out of the nodeSeq
  println(s"\nMeasurements:")
  val measurementsSeq = stationFull.map(el => fromXMLtoMeasurement(el))
  measurementsSeq.foreach(println)

  //function that helps save measurement as TSV
  //station name and European_code necessary for creating name.
  def saveMeasurementAsTSV(m: Measurement):Unit ={
    val txt = s"${m.componentName}\t${m.componentCaption}\t${m.measurementUnit}\t${m.measurementTechniquePrinciple}\t${m.mean}\t${m.median}"
    import java.io.{File, PrintWriter}
    val pw = new PrintWriter(new File(s"./src/resources/jsonFiles/${m.stationName}_${m.stationsEuropeanCode}_yearly.tsv"))
    pw.write(txt)
    pw.close()
  }

  measurementsSeq.foreach(m => saveMeasurementAsTSV(m))

  //DB section
  val url = "jdbc:sqlite:./src/resources/dataBase/Station_Data_Iceland.db"
  val connection = DriverManager.getConnection(url)

  //Comment out, when table created
  DBFunctions.migrateMeasurementsTable(connection)

  //Comment out, when all measurements saved in table
  measurementsSeq.foreach(DBFunctions.insertMeasurement(connection, _))


  }
