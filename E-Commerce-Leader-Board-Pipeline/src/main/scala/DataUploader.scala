import org.apache.spark.sql.{Dataset, Row}
import util.{AppConstants, ClientDbConnectionUtil, SparkUtil};
/**
 * This class is responsible to collect required data from client db for further processing
 */
object DataUploader {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")


// prerequisites
    val sparkUtil = new SparkUtil()

    // Setting up props for db access and other operations
    val appConstants = new  AppConstants()
    appConstants.setUpConfig()

    val clientDbUtil = new ClientDbConnectionUtil(appConstants)
    val startDate = "2020-04-01"
    val endDate = "2020-04-30"
    val jobsDbName = appConstants.props.getProperty("db.jobs");


    // creating new Spark Session
    sparkUtil.sparkCreateSession()



    // opening client DB connection
    clientDbUtil.openPostgresDbConnection(appConstants.props.getProperty("db.jobs"))


    // reading boundaries of data , to inform spark that from where it needs to start reading and where it ends
   val dataBoundaries = clientDbUtil.readJobBoundariesFromDb(startDate,endDate)


    // read Data from client db and load to spark RDD or Dataset
   var dataset = sparkUtil.sparkReadFromDb(
                            jobsDbName,
                            appConstants.props,appConstants.JOBS_READ_DATA_SQL+"'"+startDate+"' AND '"+endDate+"'",
                            dataBoundaries,"2","id")



    // SAVE RDD DATA to local file system
    sparkUtil.sparkWriteToFileSystem(jobsDbName,"status",dataset)
    println("WRITE TO LOCAL FILE SYSTEM COMPLETED!")


    sparkUtil.closeSparkSession()

  }

}
