package com.john.dw.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.john.dw.gmall.common.Constant
import com.john.dw.gmall.realtime.bean.{AlertInfo, EventLog}
import com.john.dw.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object AlertApp {
  def main(args: Array[String]): Unit = {

  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alertApp")
  val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
  // 1. 从kakfa读取数据, 并添加窗口: 窗口长度5分组, 滑动步长5s
  val sourceStream: DStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Constant.EVENT_TOPIC).window(Minutes(5))
  // 2. 封装数据
  val eventLogStream: DStream[(String, EventLog)] = sourceStream.map { case (_, jsonString) =>
    val log: EventLog = JSON.parseObject(jsonString, classOf[EventLog])
    (log.mid, log)
  }
  // 3. 处理数据(根据分析来完成逻辑编码)
  // 3.1 按照mid'分组
  val logByMidStream: DStream[(String, Iterable[EventLog])] = eventLogStream.groupByKey
  // 3.2 产生预警信息
  val alert: DStream[(Boolean, AlertInfo)] = logByMidStream.map {
    case (mid, logIt) =>
      // logIt 在这最近的5分钟内, 在mid这个设备上的所有事件日志
      // 记录: 1. 5分钟内当前设备有几个用户登录   2. 有没有点击(浏览商品)
      // 返回: 预警信息
      // 1. 记录领取过优惠券的用户  (java的set集合, scala的集合到es中看不到数据)
      val uids: util.HashSet[String] = new util.HashSet[String]()
      // 2. 记录领取的优惠券对应的商品的id
      val itemIds: util.HashSet[String] = new util.HashSet[String]()
      // 3. 记录5分钟内所有的事件
      val events: util.ArrayList[String] = new util.ArrayList[String]()
      var isClickItem: Boolean = false // 表示5分钟内有没有点击商品
      import scala.util.control.Breaks._
      breakable {
        logIt.foreach {
          eventLog =>
            events.add(eventLog.eventId) // 把事件保存下来
            // 如果事件类型是优惠券, 表示有一个用户领取了优惠券, 把这个用户保存下来
            eventLog.eventId match {
              case "coupon" =>
                uids.add(eventLog.uid)
                itemIds.add(eventLog.itemId)
              case "clickItem" =>
                isClickItem = true
                break
              case _ =>
            }
        }
      }
      (uids.size() >= 3 && !isClickItem, AlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
  }
  // 4. 把key是true的预警信息写入到es中
  import com.john.dw.gmall.realtime.util.ESUtil._
  alert.filter(_._1).map(_._2).foreachRDD(rdd => {
    rdd.saveToES("gmall0830_coupon_alert")
  })
  alert.print
  ssc.start()
  ssc.awaitTermination()
  }
}
//object HH{
//  def main(args: Array[String]): Unit = {
//    val list = List(1,2,3,4)
//    val it: Iterator[Int] = list.iterator
//    import scala.util.control.Breaks._
//    breakable{
//      it.foreach{
//        case 1 => println("1")
//        case 2 => break()
//        case 3 => println("3")
//        case 4 => println("4")
//      }
//    }
//  }
//}