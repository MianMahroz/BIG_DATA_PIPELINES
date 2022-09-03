package util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

class KProducer {


  def sendData(topic:String,key:String,value:String,props:Properties): Unit ={
    var record = new ProducerRecord[String,String](topic,key,value);

    var producer = new KafkaProducer[String,String](props)
    producer.send(record)

  }
}
