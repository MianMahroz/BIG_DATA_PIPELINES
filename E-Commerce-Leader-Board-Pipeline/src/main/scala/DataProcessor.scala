import util.{AppConstants, SparkUtil}
import org.apache.spark.sql.{Dataset, Row}


/**
 * This is responsible to read parquet files that uploader saved at /raw_data
 * Aggregate the data , generate stats and send to kafka topic for leader board consumer
 * We can also dump the data on which the stats are generated to maria db if needed.
 */
object DataProcessor {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")


    // prerequisites
    val sparkUtil = new SparkUtil()
    val fileDir = "raw_data/"
    val startDate = "2020-05-01"
    val endDate = "2020-05-30"
    val topicName = "jobs-summary";


    // Setting up props for db access and other operations
    val appConstants = new  AppConstants()
    appConstants.setUpConfig()


    // creating spark Session
    sparkUtil.sparkCreateSession()

    // Reading Data from file system parquet files that are stored by job status
   var dataset = sparkUtil.sparkReadFromFile(fileDir)
   dataset.show()

    // Creating Local Temp Table to perform aggregation using SQL
    // temp table was only accessible within one session
    dataset.createOrReplaceTempView("GLOBAL_JOBS_TABLE")

    // Verifying GLOBAL_JOBS_TABLE View creation and content
    println("Total Records available : ")
    sparkUtil.spark.sql("SELECT count(*) FROM " + "GLOBAL_JOBS_TABLE").show()

    // Aggregating Data using SPARK SQL
    var summary = sparkUtil.sparkAggregateJobsData(startDate,endDate)
    println("GLOBAL_JOBS_TABLE  Summary: ")
    summary.show()

    // Push Summary Data to a kafka topic , to be consumed by Leader Dashboard client
    sparkUtil.sparkWriteToKafkaTopic(topicName,summary,appConstants.props)

    //closing spark session
    sparkUtil.closeSparkSession()

  }
}
