package manager

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import util.AppConstants

class KafkaManager (val appConstants: AppConstants){

  def sendData (key: String,value: String): Unit ={
    var record = new ProducerRecord[String,String](appConstants.props.getProperty("stats.topic"),key,value)

    var kProducer = new KafkaProducer[String,String](appConstants.props)
    kProducer.send(record)
  }
}
