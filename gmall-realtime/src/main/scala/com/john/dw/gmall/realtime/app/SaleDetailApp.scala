package com.john.dw.gmall.realtime.app

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.john.dw.gmall.common.Constant
import com.john.dw.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.john.dw.gmall.realtime.util.{ESUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable


object SaleDetailApp {

  def cacheOrderInfo(client: Jedis, orderInfo: OrderInfo): String = {
    val key: String = "orderInfo_" + orderInfo.id
    cacheToRedis(client,key,orderInfo)
  }
  def cacheOrderDetail(client: Jedis, orderDetail: OrderDetail): String ={
    val key: String = "orderDetail_" + orderDetail.order_id + "_" + orderDetail.id
    cacheToRedis(client, key, orderDetail)
  }
  def cacheToRedis(client: Jedis, key: String, value: AnyRef): String ={
    // 需要把value变成json字符串写入到redis
    val content: String = Serialization.write(value)(DefaultFormats)
    //        client.set(key, content)
    client.setex(key,60 * 30, content)
  }

  def main(args: Array[String]): Unit = {
    // 1. 从kafka读取数据
    val conf: SparkConf = new SparkConf().setAppName("saleDetailApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    // 2. 读取order_detail 和order_info 相关的流
    val orderInfoStream: DStream[(String, OrderInfo)] = MyKafkaUtil.getKafkaStream(ssc, Constant.ORDER_TOPIC).map {
      case (_, jsonString) =>
        val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        (orderInfo.id, orderInfo)
    }
    val orderDetailStream: DStream[(String, OrderDetail)] = MyKafkaUtil.getKafkaStream(ssc, Constant.DETAIL_TOPIC).map {
      case (_, jsonString) =>
        val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
    }
    // 3. 双流join
    val fullJoinStream: DStream[SaleDetail] = orderInfoStream.fullOuterJoin(orderDetailStream).mapPartitions(it => {
      // 获取一个redis的客户端
      val client: Jedis = RedisUtil.getJedisClient
      // (orderId, (Some(orderInfo), None)) 中会对应多个SaleDetail, 所以需要使用flatMap
      val result: Iterator[SaleDetail] = it.flatMap {
        case (orderId, (Some(orderInfo), opt)) =>
          //缓存入redis中
          cacheOrderInfo(client, orderInfo)
          import scala.collection.JavaConversions._
          // 去orderDetail缓存中, 读出与当前这个orderInfo对应的OrderDetail
          val orderDetailJsonSet: mutable.Set[String] = client.keys(s"orderDetail_${orderInfo.id}_*")
          val saleDetailSet: mutable.Set[SaleDetail] = orderDetailJsonSet.filter(_.startsWith("{")).map(jsonString => {
            val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          })
          // 如果有OrderDetail, 则和orderInfo封装在一起
          if (opt.isDefined) {
            val orderDetail: OrderDetail = opt.get
            saleDetailSet += SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
          }
          saleDetailSet
        case (orderId, (None, Some(orderDetail))) =>
          //去orderInfo的缓冲读取数据, 如果读到数据,则orderDetail不需要缓存,否则才需要缓存
          val orderInfoString: String = client.get(s"orderInfo_${orderDetail.order_id}")
          if (orderInfoString != null && orderInfoString.startsWith("{")) {
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
            SaleDetail().mergeOrderDetail(orderDetail).mergeOrderInfo(orderInfo) :: Nil
          } else {
            cacheOrderDetail(client, orderDetail)
            Nil
          }
      }
      client.close()
      result
    })
    fullJoinStream
    // 3. 反查mysql, 补齐User的相关信息
    // sparksql ds df    rdd->df
    // 从jdbc读userInfo的数据
    val jdbcUrl = "jdbc:mysql//hadoop201:3306/gmall0830"
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","000000")
    val resultRdd: DStream[SaleDetail] = fullJoinStream.transform(rdd => {
      // 从jdbc读到user信息
      val userInfoRdd: RDD[(String, UserInfo)] = spark.read.jdbc(jdbcUrl, "user_info", prop).as[UserInfo].rdd.map(userInfo => (userInfo.id, userInfo))
      // orderinfo和orderDetail已经合并的信息
      val saleDetailRdd: RDD[(String, SaleDetail)] = rdd.map(saleDetail => (saleDetail.user_id, saleDetail))
      saleDetailRdd.join(userInfoRdd).map {
        case (userId, (saleDetail, userInfo)) =>
          saleDetail.mergeUserInfo(userInfo)
      }
    })
    resultRdd.foreachRDD(rdd => {
      ESUtil.insertBulk(Constant.SALE_DETAIL_INDEX, rdd.collect().toIterator)
    })
    ssc.start()
    ssc.awaitTermination();
  }
}


























