package manager

import util.AppConstants

import java.sql.{Connection, DriverManager, SQLException}

class MariaDbManager(val appConstants: AppConstants) {

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
}
