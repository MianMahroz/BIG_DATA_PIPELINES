package manager

import util.AppConstants

import java.sql.{Connection, DriverManager, SQLException}

class MariaDbManager(val appConstants: AppConstants)  extends Serializable{

  var mariaDbConn: Connection = null;

  @throws[SQLException]
  @throws[ClassNotFoundException]
  def openConnection(dbName:String): Connection = {
    val db_url = appConstants.props.get("db.url").toString + dbName
    val db_user = appConstants.props.get("db.user").toString
    val db_pass = appConstants.props.get("db.pass").toString
    Class.forName("org.mariadb.jdbc.Driver")

    mariaDbConn = DriverManager.getConnection(db_url, db_user, db_pass)
    println("MARIA DB CONNECTION OPENED!")
    mariaDbConn
  }

  @throws[SQLException]
  def closeDbConnection(): Unit = {
    mariaDbConn.close()
    println("DB CONNECTION CLOSED!")
  }


  //Used by StreamingAnalytics for writing data into MariaDB
  def insertSummary(timestamp: String, lastAction: String, duration: String): Unit = {
    try {
      val sql = "INSERT INTO visit_summary_stats " + "(INTERVAL_TIMESTAMP, LAST_ACTION, DURATION) VALUES " + "( '" + timestamp + "'," + " '" + lastAction + "'," + duration + ")"
      //System.out.println(sql);
      mariaDbConn.createStatement.execute(sql)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


}
