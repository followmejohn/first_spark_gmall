package com.john.dw.gmall.realtime.bean

import java.text.SimpleDateFormat


case class StartupLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      logType: String,
                      version: String,
                      ts: Long,
                      var logDate: String = null, // 2020-02-11
                      var logHour: String = null) { // 1 2 10 12  小时
  private val f1 = new SimpleDateFormat("yyyy-MM-dd")
  private val f2 = new SimpleDateFormat("HH")
  logDate = f1.format(ts)
  logHour = f2.format(ts)
}
//object TT{
//  def main(args: Array[String]): Unit = {
//    val ts: Long = System.currentTimeMillis()
//    println(ts)//1581425421646
//    val f1 = new SimpleDateFormat("yyyy-MM-dd")
//    val f2 = new SimpleDateFormat("HH:mm:ss")
//    println(f1.format(ts))//2020-02-11
//    println(f2.format(ts))//20:50:21
//  }
//}
/*
{"logType":"startup","area":"hebei",
"uid":"7546","os":"ios","appId":"gmall",
"channel":"xiaomi","mid":"mid_351","version":"1.2.0","ts":1581402728428}
 */
