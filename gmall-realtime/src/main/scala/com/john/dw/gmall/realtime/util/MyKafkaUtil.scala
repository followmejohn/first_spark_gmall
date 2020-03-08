package com.john.dw.gmall.realtime.util

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkaUtil {
  def getKafkaStream(ssc:StreamingContext, topic: String): InputDStream[(String, String)] ={
    val parms: Map[String, String] = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Util.getProperty("kafka.broker.list"),
      ConsumerConfig.GROUP_ID_CONFIG -> Util.getProperty("kafka.group")
    )
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      parms,
      Set(topic)
    )
  }
}
