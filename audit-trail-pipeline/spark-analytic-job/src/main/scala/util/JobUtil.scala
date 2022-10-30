package util

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}

class JobUtil {


  /**
   * Convert String Binary Data to required Json
   * Data sample is as below
   * {"schema":{"type":"string","optional":false},"payload":"{\"_id\": {\"$numberLong\": \"-756628262192416819\"}, \"type\": \"SIGNUP\", \"details\": {\"name\": \"PRODUCT_DETAILS\", \"action\": \"VIEW\", \"userId\": \" 6890617409429601930\", \"productId\": \" 6120941396638220364\", \"_class\": \"com.audit.trail.datagenerator.model.EventDetails\"}, \"_class\": \"com.audit.trail.datagenerator.model.EventModel\"}"}
   * @param df
   * @return
   */
  def convertToJson(df: DataFrame): DataFrame = {

    // Defining Data Structure
    val payloadDetailsObj = new StructType()
      .add("type",StringType )
      .add("name",StringType )
      .add("action",StringType )
      .add("userId",StringType )
      .add("productId",StringType )


    val payloadObj = new StructType()
      .add("type",StringType )
      .add("details", payloadDetailsObj)

    val schema = new StructType()
      .add("payload", StringType)

    // Extracting Data by defined schema
    var visitStats = df
       // Binary to String
      .selectExpr("CAST(value AS STRING) as value")

      // String to JSON
      .select(functions.from_json(
        functions.col("value"),schema).as("event"))

      // Payload String to JSON
      .select(functions.from_json(
        functions.col("event.payload"),payloadObj).as("payload"))

      // Selecting Required Fields ONLY
      .select("payload.type","payload.details");

    visitStats.printSchema()
    visitStats.show(5,false)
    visitStats
  }
}
