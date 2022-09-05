package manager

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import util.AppConstants

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

    // As data is in binary , so we need to convert into String
    val visitStats = df
      .selectExpr("CAST(value as String)").as("value") // converting from byte to String
      .select(from_json(col("value"), schema)).as("data") // collecting Json from string as per given schema
      .select("data.*") // selecting all columns or you can specify particular col i.e data.country,data.duration etc

    visitStats
  }
}
