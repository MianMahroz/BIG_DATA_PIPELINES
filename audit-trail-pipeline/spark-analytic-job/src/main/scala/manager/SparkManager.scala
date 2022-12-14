package manager

import main.scala.KStreamListener.props
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession, functions}
import org.apache.spark.sql.Dataset

import java.time.LocalDate

/**
 * This class managing all operations related to spark
 */
class SparkManager {

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
   * Closing Spark Session
   */
  def closeSparkSession(): Unit = {
    spark.close()
    println("SPARK SESSION CLOSED")
  }


  /**
   * Reading Real time Stream from Kapka topic
   * @return
   */
  def sparkReadStreamFromTopic(): DataFrame = {
    println("READING DATA FROM TOPIC")

    var df =  spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "mongo-01-.eventlog.auditlog")
      .load();

    println(s"Data RECEIVED: ${df.count()}")

    df
  }


  def aggregateAuditLogs(df: DataFrame): DataFrame ={
   println("AGGREGATING AUDIT LOG DATA....")

   val summaryDF = spark.sql(
      "SELECT " +
        "details.productId,details.name,details.action,COUNT(*) AS count " +
        "FROM AUDIT_TABLE " +
        "WHERE to_date(created_at)==current_date() " +
        "GROUP BY details.productId,details.name,details.action "
    )

    summaryDF.show(5,false)
    summaryDF
  }

  def pushSummaryDataToKafka(df:DataFrame): Unit ={
    println("WRITING DAILY SUMMARY TO KAFKA TOPIC......")

    df
      .selectExpr("CAST(name AS STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic",s"${LocalDate.now()}-eventlog-summary")
      .option("minPartitions",3)
      .save()
  }

  def dumpSummaryDataToMariaDb(df:DataFrame):Unit ={
    println("Saving data to mariadb.....")

    df
      .write
      .format("jdbc")
      .option("url", props.getProperty("db.url"))
      .option("dbtable", props.getProperty("db.table"))
      .option("user", props.getProperty("db.user"))
      .option("password", props.getProperty("db.pass"))
      .option("driver",props.getProperty("db.driver"))
      .save()
  }




}
