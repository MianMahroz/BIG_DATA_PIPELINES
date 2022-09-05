import DataGenerator.appConstants
import manager.{KafkaManager, SparkManager}
import org.apache.spark.sql.{Dataset, ForeachWriter, Row}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import util.AppConstants

/**
 * This class listen to kafka stream , and process analytics for 5 second wnindow
 */
object KStreamListener {

  val sparkManager = new SparkManager()
  val appConstants = new AppConstants
  val kafkaManager = new KafkaManager(appConstants)


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    // setting up environment variables
    appConstants.setUpConfig()

    // creating spark session
    sparkManager.sparkCreateSession()

    // Getting Data from topic
    val rawVisitStatsDF =  sparkManager.sparkReadFromTopic();
    rawVisitStatsDF
      .printSchema()

    // As data is in binary , so we need to convert into String
    val visitStats = kafkaManager.convertToJson(rawVisitStatsDF)

    // Listening to new messages
   sparkManager.sparkListenToStream(visitStats)


    sparkManager.closeSparkSession()

  }

}
