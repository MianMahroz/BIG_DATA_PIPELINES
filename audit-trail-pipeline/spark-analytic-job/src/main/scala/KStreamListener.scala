package main.scala

import manager.SparkManager
import util.{AppConstants, JobUtil}

import java.util.Properties

object KStreamListener {

  val sparkManager = new SparkManager()
  val util = new JobUtil()
  val appConstants = new AppConstants()
  var props = new Properties()

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

    //setup props
    appConstants.setUpConfig()
    props = appConstants.props

    // creating spark session
    sparkManager.sparkCreateSession()

    // Read Event Stream From Kafka Topic
    val rawDataFromKafka =  sparkManager.sparkReadStreamFromTopic();

    // Converting Binary Data From Kafka to Json for processing
    val auditLogDF = util.convertToJson(rawDataFromKafka)


    // To Use SparkSQL for data aggregation , creating a temp table
    auditLogDF.createOrReplaceTempView("AUDIT_TABLE");

    // Aggregate Data and extract today`s analytics
    val summaryDF = sparkManager.aggregateAuditLogs(auditLogDF)

    // Push today analytics to kafka topic to be consumed by leader board
    sparkManager.pushSummaryDataToKafka(summaryDF)

    // Dump Aggregated Data to mysql for cross reference
   sparkManager.dumpSummaryDataToMariaDb(summaryDF)


    // closing Session
    sparkManager.closeSparkSession()

  }


}