package main.scala

import manager.SparkManager
import util.JobUtil

object KStreamListener {

  val sparkManager = new SparkManager()
  val util = new JobUtil()

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    // creating spark session
    sparkManager.sparkCreateSession()

    // Read Event Stream From Kafka Topic
    val rawDataFromKafka =  sparkManager.sparkReadStreamFromTopic();

    // Converting Binary Data From Kafka to Json for processing
    val auditLogDF = util.convertToJson(rawDataFromKafka)

    // closing Session
    sparkManager.closeSparkSession()

  }


}