package com.john.dw.gmall.canal

import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.john.dw.gmall.common.Constant


object CanalHandler {
  import scala.collection.JavaConversions._
  //解析数据
  def handle(tableNmae: String,rowDatas: util.List[CanalEntry.RowData],eventType: CanalEntry.EventType) ={
    if(rowDatas != null && !rowDatas.isEmpty && "order_info" == tableNmae && eventType == EventType.INSERT){
      for(rowData <- rowDatas){
        val jsonObj: JSONObject = new JSONObject()
        // 获取到变化后的所有的列
        val columns: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
        for(column <- columns){
          val key: String = column.getName
          val value: String = column.getValue
          jsonObj.put(key,value)
        }
//          println(jsonObj.toString)
        // 得到一个kafka的生产者, 通过生成这向kafka写数据
        MyKafkaUtils.send(Constant.ORDER_TOPIC,jsonObj.toString)
      }
    }
  }

}
