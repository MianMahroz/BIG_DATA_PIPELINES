package util

import java.util.Properties

class AppConstants extends Serializable{


  var props = new Properties();




  def setUpConfig(): Unit = {
    props.setProperty("db.url", "jdbc:mysql://localhost:3306/eventlog_summary")
    props.setProperty("db.user", "root")
    props.setProperty("db.pass", "spark")
    props.setProperty("db.name", "eventlog_summary")
    props.setProperty("db.table", "audit_stats")
    props.setProperty("db.driver", "com.mysql.cj.jdbc.Driver")


  }
}
