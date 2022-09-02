package util

import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.Properties

/**
 * This class is responsible for managing every function that requires client db interaction
 */
class ClientDbConnectionUtil(appConstants: AppConstants) {


  var clientDbConn: Connection = null


  @throws[SQLException]
  @throws[ClassNotFoundException]
  def openPostgresDbConnection(dbName: String): Connection = {
    val db_url = appConstants.props.get("db.url").toString + dbName
    val db_user = appConstants.props.get("db.user").toString
    val db_pass = appConstants.props.get("db.pass").toString
    Class.forName("org.postgresql.Driver")
    clientDbConn = DriverManager.getConnection(db_url, db_user, db_pass)
    println("CLIENT DB CONNECTION OPENED!")
    clientDbConn
  }

  @throws[SQLException]
  def closeDbConnection(): Unit = {
    clientDbConn.close()
  }

  def readJobBoundariesFromDb(startDate:String,endDate:String): DataBoundaryDto ={


    var stmt = clientDbConn.prepareStatement(appConstants.JOBS_READ_BOUNDARY_SQL+"'"+startDate+"' AND '"+endDate+"';");
    var rs = stmt.executeQuery();
    var boundDto = new DataBoundaryDto();

    // Reading from result set and initializing the boundary obj
    while (rs.next()){
       boundDto.minBound = rs.getInt(1)
       boundDto.maxBound = rs.getInt(2)
    }

    println("DATA BOUNDARIES: "+boundDto.toString)

    // returning boundDto
    boundDto
  }

}
