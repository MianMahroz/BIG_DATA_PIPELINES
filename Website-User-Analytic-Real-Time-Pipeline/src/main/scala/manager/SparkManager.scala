package manager

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.*

import java.util.Properties
/**
 * This class manages all operations related to apache spark
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


  def sparkReadStreamFromTopic(): DataFrame = {
    var df =  spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark.streaming.website.visits")
      .option("startingOffsets","latest")  // earliest/latest
      .load();

    df
  }

  def sparkWriteStreamToTopic(df: DataFrame,topicName:String): Unit ={
    //Write data frame to an outgoing Kafka topic.
    df
//      .selectExpr("CAST(columnName AS STRING) AS key", "to_json(struct(*)) AS value")  //you can also use this if key is needed
      .selectExpr("format_string(\"%s,%s,%s,%s\", visitDate,country,lastAction,duration) as value")
      .writeStream
      .format("kafka")
      .option("checkpointLocation", "tmp/cp-shoppingcart2")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", topicName)
      .start();
  }

  def sparkStartStreamingDataFrame(df: DataFrame): Unit ={
    df
      .writeStream
      .format("console")
      .outputMode("append")
      .foreach(new ForeachWriter[Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = {
        true
      }

      override def process(value: Row): Unit = {
        println("RECEIVING DATA :"+value)
      }

      override def close(errorOrNull: Throwable): Unit = {

      }
    })
      .start()
    //.awaitTermination()
  }


  def sparkCreateDataWindow(df: DataFrame): DataFrame = {
    val summary = df
      .withColumn("timestamp", functions.current_timestamp()) // add new column in df and initialize it
      .withWatermark("timestamp", "5 seconds") // A watermark tracks a point in time before which we assume no more late data is going to arrive.
      .groupBy(functions.window(
        functions.col("timestamp"),
        "5 seconds"),
        functions.col("lastAction"))
      .agg(functions.sum(functions.col("duration")));

    summary
  }



  def closeSparkSession(): Unit = {
    spark.close()
    println("SPARK SESSION CLOSED")
  }

}
