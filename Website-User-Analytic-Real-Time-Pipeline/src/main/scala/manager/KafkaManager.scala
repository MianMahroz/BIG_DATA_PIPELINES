package manager

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import util.AppConstants
import org.apache.spark.sql._


class KafkaManager (val appConstants: AppConstants){

  def sendData (key: String,value: String): Unit ={
    var record = new ProducerRecord[String,String](appConstants.props.getProperty("stats.topic"),key,value)

    var kProducer = new KafkaProducer[String,String](appConstants.props)
    kProducer.send(record)
  }


  def convertToJson(df: DataFrame): DataFrame = {
    // defining schema of kafka topic data
    val schema = new StructType()
      .add("visitDate", StringType)
      .add("country", StringType)
      .add("lastAction", StringType)
      .add("duration", StringType );

    var visitStats = df
      .selectExpr("CAST(value AS STRING) as value")
      .select(functions.from_json(
        functions.col("value"),schema).as("visits"))
      .select("visits.visitDate",
        "visits.country",
        "visits.lastAction",
        "visits.duration");

    visitStats.printSchema()
    visitStats
  }
}
