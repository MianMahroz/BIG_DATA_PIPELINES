package manager

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

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


  def sparkReadFromTopic(): DataFrame = {
    var df =  spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark.streaming.website.visits")
      .option("startingOffsets","earliest")  // earliest/latest
      .load();

    df
  }

  def sparkListenToStream(df: DataFrame): Unit ={
    df
      .writeStream
      .format("console")
      .outputMode("append")
      .foreach(new ForeachWriter[Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = {
        true
      }

      override def process(value: Row): Unit = {
        println("STREAM LISTENING"+value)
      }

      override def close(errorOrNull: Throwable): Unit = {

      }
    })
      .start()
      .awaitTermination()
  }

  def closeSparkSession(): Unit = {
    spark.close()
    println("SPARK SESSION CLOSED")
  }




}
