package util

import java.util.Properties

class AppConstants {

  var CREATE_DB_SQL = "CREATE DATABASE "
  var CREATE_USER_INTERACTIONS_TABLE_SQL ="" +
    "CREATE TABLE `website_stats`.`visit_stats`" +
    " (`ID` int(11) NOT NULL AUTO_INCREMENT, " +
    "`INTERVAL_TIMESTAMP` DATETIME DEFAULT NULL,`LAST_ACTION` varchar(45) DEFAULT NULL," +
    "`DURATION` int(10) DEFAULT NULL,  PRIMARY KEY (`ID`)) "


  var props = new Properties();




  def setUpConfig(): Unit = {
    props.setProperty("db.url", "jdbc:mysql://localhost:3306/")
    props.setProperty("db.user", "root")
    props.setProperty("db.pass", "spark")
    props.setProperty("db.name", "website_stats")
    props.setProperty("db.tableName", "visit_stats")


    // producer props
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    /**
     * in case when custom serializer is defined
     */
    //  props.put("value.serializer","dto.JobSummarySerializer")

    props.put("acks","all")
    props.put("stats.topic","visit_stats_topic")

  }
}
