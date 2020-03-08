package com.john.dw.gmall.canal

import java.net.{InetSocketAddress, SocketAddress}
import java.util

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.google.protobuf.ByteString

object CanalClient {
  def main(args: Array[String]): Unit = {
    // 可以把java的集合转换成scala的集合, 这样就可以对java的集合使用scala的遍历语法
    import scala.collection.JavaConversions._
    val address: SocketAddress = new InetSocketAddress("hadoop201",11111)
    // 1. 创建一个canal的客户端连接器
    val connector: CanalConnector = CanalConnectors.newSingleConnector(address,"example","","")
    // 2. 连接到canal实例
    connector.connect()
    // 订阅数据, example实例实际监控了很多的数据, 但是我们的应用只需要其中的一部分
    connector.subscribe("gmall0830.*")// 获取gmall0830这个数据库下的所有的表
    // 3. 获取数据
    while(true){// 轮询的方式从canal来获取数据
      val mes: Message = connector.get(100)// 100 表示最多有多少条sql语句导致的变化的信息
      val entries: util.List[CanalEntry.Entry] = if (mes != null) mes.getEntries else null
      if(entries != null && !entries.isEmpty){
        for(entry <- entries){
          // Entry的类型必须是RowData 不能是事务的开始, 结束等其他类型
          if(entry != null && entry.getEntryType == EntryType.ROWDATA){
            val storeValue: ByteString = entry.getStoreValue
            val rowChange: RowChange = RowChange.parseFrom(storeValue)
            val rowDatas: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
            // 1. 表名  2. 每行的数据  3. RowChange的事件类型(insert , update, delete,...)
            CanalHandler.handle(entry.getHeader.getTableName,rowDatas,rowChange.getEventType)
          }
        }
      }else {
        println("没有拉取到数据, 2s后继续...")
        Thread.sleep(2000)
      }
    }
  }
}
