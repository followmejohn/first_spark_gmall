package com.john.dw.gmall.canal

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

object MyKafkaUtils {
  private val properties: Properties = new Properties()
  properties.put("bootstrap.servers","hadoop201:9092,hadoop202:9092,hadoop203:9092")
  properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
  private val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](properties)
  def send(topic: String, content: String):Any ={
    producer.send(new ProducerRecord[String,String](topic,content))
  }
}
