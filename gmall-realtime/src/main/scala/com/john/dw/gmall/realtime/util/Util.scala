package com.john.dw.gmall.realtime.util

import java.io.InputStream
import java.util.Properties


object Util{
  private val stream: InputStream = Util.getClass.getClassLoader.getResourceAsStream("config.properties")
  private val properties: Properties = new Properties()
  properties.load(stream)
  def getProperty(proName: String): String ={
    properties.getProperty(proName)
  }
}
