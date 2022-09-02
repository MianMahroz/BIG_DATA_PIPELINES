package util;

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import java.util.Properties

/**
 *  THis util class responsible for all interactions related to spark
 */
class SparkUtil {

  var spark: SparkSession = null

  /**
   * Creates new Spark session
   *
   * @return
   */
   def sparkCreateSession(): SparkSession = {
    val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.default.parallelism", 2)
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
      .config("spark.eventLog.enabled", true)
      .config("spark.rdd.compress", "true")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .appName("StockDailyUploaderJob")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    spark = sparkSession
    System.out.println("SPARK SESSION CREATED!")
    spark

  }

  /**
   * Reading data from local warehouse and saving it to temporary file location using spark default RDD (Resilient Distributed Dataset)
   */
  def sparkReadFromDb(dbName: String, props: Properties, sqlQuery: String, boundary: DataBoundaryDto, partitionCount: String, partitionColumn: String): Dataset[Row] = {
    System.out.println("SPARK READING FROM DB " + sqlQuery)
    val dataFrame: Dataset[Row] = spark
      .read
      .format("jdbc")
      .option("url", props.getProperty("db.url") + dbName)
      .option("dbtable", "( " + sqlQuery + " ) as tmpStock")
      .option("user", props.getProperty("db.user"))
      .option("password", props.getProperty("db.pass"))
      .option("partitionColumn", partitionColumn)
      .option("lowerBound", boundary.minBound)
      .option("upperBound", boundary.maxBound + 1)
      .option("numPartitions", partitionCount)
      .load()

    println("Total size of Stock DF : " + dataFrame.count)
    dataFrame.show()
    dataFrame
  }

  /**
   * Saving data to local file systems using spark default RDD (Resilient Distributed Dataset)
   * @param dbName
   * @param dataFrame
   */
  def sparkWriteToFileSystem(dbName: String, partitionBy:String,dataFrame: Dataset[Row]): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .partitionBy(partitionBy)
      .parquet("raw_data/" + dbName)
  }

  def closeSparkSession(): Unit = {
    spark.close()
  }


  def sparkReadFromFile(sourceDir: String): Dataset[Row] = {
    val dataset: Dataset[Row] = spark.read.parquet(sourceDir)
    return dataset
  }

  def sparkAggregateJobsData(startDate:String,endDate:String): Dataset[Row] = {

    var sql ="select " +
      "(Select COUNT(id) from GLOBAL_JOBS_TABLE where status='OPEN' AND created BETWEEN " + "'"+startDate + "' AND '"+endDate + "') as TOTAL_OPEN_JOBS," +
      "(Select COUNT(id) from GLOBAL_JOBS_TABLE where status='COMPLETED' AND created BETWEEN " + "'"+startDate + "' AND '"+endDate + "') as TOTAL_COMPLETED_JOBS," +
      "(Select COUNT(id) from GLOBAL_JOBS_TABLE where status='ASSIGNED' AND created BETWEEN " + "'"+startDate + "' AND '"+endDate + "') as TOTAL_ASSIGNED_JOBS," +
      "(Select COUNT(id) from GLOBAL_JOBS_TABLE where status='DRAFT' AND created BETWEEN " + "'"+startDate + "' AND '"+endDate + "') as TOTAL_DRAFT_JOBS," +
      "(Select SUM(budget_figure) from GLOBAL_JOBS_TABLE where status='OPEN' AND created BETWEEN " + "'"+startDate + "' AND '"+endDate + "') as TOTAL_OPEN_JOBS_BUDGET," +
      "(Select SUM(budget_figure) from GLOBAL_JOBS_TABLE where status='COMPLETED' AND created BETWEEN " + "'"+startDate + "' AND '"+endDate + "') as TOTAL_COMPLETED_JOBS_BUDGET," +
      "(Select SUM(budget_figure) from GLOBAL_JOBS_TABLE where status='ASSIGNED' AND created BETWEEN " + "'"+startDate + "' AND '"+endDate + "') as TOTAL_ASSIGNED_JOBS_BUDGET," +
      "(Select SUM(budget_figure) from GLOBAL_JOBS_TABLE where status='DRAFT' AND created BETWEEN " + "'"+startDate + "' AND '"+endDate + "') as TOTAL_DRAFT_JOBS_BUDGET" +
      " from GLOBAL_JOBS_TABLE job limit 1"

    val stockSummary: Dataset[Row] = spark.sql(sql)
    stockSummary
  }


}
