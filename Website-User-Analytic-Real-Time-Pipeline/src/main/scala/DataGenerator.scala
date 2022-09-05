import manager.{KafkaManager, MariaDbManager}
import util.AppConstants

import java.sql.Connection
import java.util
import scala.util.Random

/**
 * This class responsible to generate raw website data for this case case.
 */
object DataGenerator  {

  val appConstants:AppConstants = new AppConstants();
  val kafkaManager: KafkaManager = new KafkaManager(appConstants)
  val mariaDbManager = new MariaDbManager(appConstants)
  var dbConn:Connection = null

  def main(args: Array[String]): Unit = {

    // setting up environment variables
    appConstants.setUpConfig()

    dbConn = mariaDbManager.openConnection("mysql") // giving dbName that we sure is exist , also later on used to fetch schema names

    initiateGenerator()

    mariaDbManager.closeDbConnection()
  }

  def initiateGenerator(): Unit ={
    setUpDbIfNotExist()
    generateData()
  }

  def setUpDbIfNotExist(): Unit = {

    // getting list of db`s we have
    var rs = dbConn.getMetaData().getSchemas
    var schemaList = new util.ArrayList[String]()

    while (rs.next()) {
      schemaList.add(rs.getString(1)) // column 1 is the schema Name
    }

    // create db if now already exist
    if (!schemaList.contains(appConstants.props.getProperty("db.name"))) {
      var stmt = dbConn.createStatement();
      stmt.executeUpdate(appConstants.CREATE_DB_SQL + appConstants.props.getProperty("db.name"));
      println("WEBSITE STATS DB CREATED!")


      // creating table to record user interactions
      stmt.executeUpdate(appConstants.CREATE_USER_INTERACTIONS_TABLE_SQL)

    }
  }


    def generateData(): Unit ={

      var random  = new Random()

      var countryList = List[String]("UAE","INDIA","QATAR","GERMANY")
      var lastActionList =  List[String]("FAQ","SHOPPING_CART","LISTING_PAGE","CONFIGURATION_PAGE")

      // <- this operator is called generator as it generates values till 100 starting from 0 and assign to i
      for (i <- 0 until 100) {
        var lastAction = lastActionList(random.nextInt(lastActionList.size))
        var country = countryList(random.nextInt(countryList.size))
        var duration  = System.currentTimeMillis()
        var visitDate = String.valueOf(System.currentTimeMillis())


        // creating json for value
        var value = "{\"visitDate\":\"" + visitDate + "\","
          +  "\"country\":\"" + country + "\","
          +  "\"lastAction\":\"" + lastAction + "\","
          +  "\"duration\":" + duration + "}" ;

        // considering country as key so each country data will be in separate partition and in ordered as well
        // data with same key end up in same partition
          kafkaManager.sendData(country,value)

      }
      println("RAW DATA OF STATS PUSHED TO KAFKA TOPIC SUCCESSFULLY!")

    }




}
