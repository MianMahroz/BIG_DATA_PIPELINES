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
//      .config("spark.eventLog.enabled", true)
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


  def sparkReadFromFile(dbName: String, sourceDir: String): Dataset[Row] = {
    val dataset: Dataset[Row] = spark.read.parquet(sourceDir)
    return dataset
  }

  def sparkAggregateData(tableName: String): Dataset[Row] = {
    val stockSummary: Dataset[Row] = spark.sql("SELECT STOCK_DATE, ITEM_NAME, " + "COUNT(*) as TOTAL_REC," + "SUM(OPENING_STOCK) as OPENING_STOCK, " + "SUM(RECEIPTS) as RECEIPTS, " + "SUM(ISSUES) as ISSUES, " + "SUM( OPENING_STOCK + RECEIPTS - ISSUES) as CLOSING_STOCK, " + "SUM( (OPENING_STOCK + RECEIPTS - ISSUES) * UNIT_VALUE ) as CLOSING_VALUE " + "FROM  " + tableName + " GROUP BY STOCK_DATE, ITEM_NAME")
    System.out.println("Global Stock Summary: ")
    stockSummary.show()
    return stockSummary
  }


}
