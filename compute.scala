package com.luxoft.sensor

import scala.io.Source

object compute {
  def main(args: Array[String]): Unit = {
    val dir=args(0)
    val fileList = factory.getListOfFiles(dir)
    val masterMap = factory.transformSensorDataToMap(fileList)
    println("Num of processed files: " +fileList.size)
    println("Num of processed measurements: " +masterMap("numProcessedMeasurements"))
    println("Num of failed measurements: " +masterMap("numFailedMeasurements"))
    
    val calculationsMap =factory.findSensorStats(masterMap)
    println("Sensors with highest avg humidity:")
    println("")
    println("sensor-id,min,avg,max")
    var statsMap=factory.findSensorStats(masterMap)
    statsMap.foreach(x=>
      println(x.toString.replace("(", "").replace(")", "")))
  }
}
