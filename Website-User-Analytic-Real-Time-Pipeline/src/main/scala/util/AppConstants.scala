package util

import java.util.Properties

class AppConstants extends Serializable{

  val MARIA_DB_NAME: String = "website_stats"
  var CREATE_DB_SQL = "CREATE DATABASE "

  var VISIT_SUMMARY_STATS_TABLE_SQL ="" +
    "CREATE TABLE `website_stats`.`visit_summary_stats`" +
    " (`ID` int(11) NOT NULL AUTO_INCREMENT, " +
    "`INTERVAL_TIMESTAMP` varchar(100),`LAST_ACTION` varchar(45) DEFAULT NULL," +
    "`DURATION` varchar(100) DEFAULT NULL,  PRIMARY KEY (`ID`)) "





  var props = new Properties();




  def setUpConfig(): Unit = {
    props.setProperty("db.url", "jdbc:mysql://localhost:3306/")
    props.setProperty("db.user", "root")
    props.setProperty("db.pass", "spark")
    props.setProperty("db.name", "website_stats")
    props.setProperty("db.tableName", "visit_stats")
    props.setProperty("db.summaryTableName", "visit_stats")



    // producer props
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    /**
     * in case when custom serializer is defined
     */
    //  props.put("value.serializer","dto.JobSummarySerializer")

    props.put("acks","all")
    props.put("stats.topic","spark.streaming.website.visits")

  }
}
