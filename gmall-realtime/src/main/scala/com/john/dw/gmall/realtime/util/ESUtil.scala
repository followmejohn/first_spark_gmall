package com.john.dw.gmall.realtime.util

import com.john.dw.gmall.realtime.bean.AlertInfo
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.rdd.RDD


object ESUtil {
  val esUrl = "http://hadoop203:9200"
  private val factory = new JestClientFactory
  private val config: HttpClientConfig = new HttpClientConfig.Builder(esUrl)
    .maxTotalConnection(100)
    .connTimeout(1000 * 100)
    .readTimeout(1000 * 100)
    .multiThreaded(true)
    .build()
  factory.setHttpClientConfig(config)
  // 返回一个客户端
  def getClient: JestClient = factory.getObject
  Traversable
  Iterator
  // 批量插入
  def insertBulk(index: String, sources: Iterator[_]): Unit ={
    if (sources.isEmpty) return
    val client: JestClient = getClient
    val builder: Bulk.Builder = new Bulk.Builder().defaultIndex(index).defaultType("_doc")
    sources.foreach{
      case (s, id: String) =>
        val action: Index = new Index.Builder(s).id(id).build()
        builder.addAction(action)
      case s =>
        val action: Index = new Index.Builder(s).build()
        builder.addAction(action)
    }
    client.execute(builder.build())
    client.shutdownClient()
  }
  //插入单条数据
  def insertSingle(index: String, source: Any, id: String = null): Unit = {
    if(source == null) return
    val client: JestClient = getClient
    val action: Index = new Index.Builder(source).index(index).`type`("_doc").id(id).build()
    client.execute(action)
    client.shutdownClient()
  }
  //隐士类
  implicit class ESFuntion(rdd: RDD[AlertInfo]){
    def saveToES(index: String): Unit ={
      rdd.foreachPartition(it => {
        val result: Iterator[(AlertInfo, String)] = it.map(info => (info, info.mid + "_" + info.ts / 1000 / 60))
        insertBulk(index,result)
      })
    }
  }
}
