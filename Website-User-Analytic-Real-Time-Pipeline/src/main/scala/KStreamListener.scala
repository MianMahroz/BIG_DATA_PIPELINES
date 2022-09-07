import DataGenerator.appConstants
import manager.{KafkaManager, MariaDbManager, SparkManager}
import org.apache.spark.sql.{Dataset, ForeachWriter, Row}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import util.AppConstants
import writer.MariaDbWriter
import java.util.concurrent.CountDownLatch

/**
 * This class listen to kafka stream , and process analytics for 5 second window
 */
object KStreamListener {

  val sparkManager = new SparkManager()
  val appConstants = new AppConstants
  val kafkaManager = new KafkaManager(appConstants)
  val mariaDbManager = new MariaDbManager(appConstants)


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    // setting up environment variables
    appConstants.setUpConfig()

    // creating spark session
    sparkManager.sparkCreateSession()

    // Getting Data from topic
    val rawVisitStatsDF =  sparkManager.sparkReadStreamFromTopic();

    // As data is in binary , so we need to convert into String
    val visitStatsDF = kafkaManager.convertToJson(rawVisitStatsDF)


    // Listening to new messages
   sparkManager.sparkStartStreamingDataFrame(visitStatsDF)

    // Filtering Shopping Cart actions for generate abandon cart items
    val shoppingCartStats = visitStatsDF.filter("lastAction == 'SHOPPING_CART'")
    sparkManager.sparkWriteStreamToTopic(shoppingCartStats,"spark.streaming.carts.abandoned")


    // Create window of 5 seconds , collect , aggregate , generate summary and write to db
    val dataSummary = sparkManager.sparkCreateDataWindow(visitStatsDF)
    dataSummary
      .writeStream
      .foreach(new MariaDbWriter(mariaDbManager, dbName = appConstants.MARIA_DB_NAME)).start()  // writing summary to db for leader board consumer




    // Below is to keep program going. you can also use .start().awaitTermination()
    val latch: CountDownLatch = new CountDownLatch(1)
    latch.await


    // closing spark session
     sparkManager.closeSparkSession()

  }

}
