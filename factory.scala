package com.luxoft.sensor

import java.io.File
import scala.io.Source
import scala.collection.immutable.ListMap

object factory {
  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.getPath).toList
  }

  def csvReader(csvPath: String): List[String] = {

    val list = Source.fromFile(csvPath).getLines.drop(1).toList
    list
  }

  def transformSensorDataToMap(fileList: List[String]): Map[String, String] = {
    var map = Map[String, String]()
    var numProcessMeasurements = 0
    var numFailedMeasurements = 0
    fileList.foreach { csvPath =>
      val list = factory.csvReader(csvPath)
      list.foreach { line =>
        val array = line.split(",")
        val key = array(0)
        val value = array(1)
        if (value.equals("NaN")) {
          numFailedMeasurements += 1
          numProcessMeasurements += 1
        } else {
          numProcessMeasurements += 1
        }
        if (map.contains(key)) {
          val oldValue = map(key)
          val newValue = oldValue + "," + value
          map += (key -> newValue)
        } else {
          map += (key -> value)
        }
      }
    }
    map += ("numProcessedMeasurements" -> numProcessMeasurements.toString)
    map += ("numFailedMeasurements" -> numFailedMeasurements.toString)
    map
  }

  def findSensorStats(masterMap: Map[String, String]): Map[String, String] = {
    var newMap = masterMap -- Set("numProcessedMeasurements", "numFailedMeasurements")
    var statsMap = Map[String, String]()
    newMap.keys.foreach { key =>
      if (newMap(key).contentEquals("NaN")) {
        statsMap += (key -> "NaN,NaN,NaN")
      } else {
        var value = newMap(key).split(",").toList
        if (value.contains("NaN")) {
          value = value.filter(_ < "NaN")
        }
        val newValue = value.map((s: String) => s.toInt)
        val min = newValue.min
        val avg = newValue.sum / newValue.size
        val max = newValue.max
        val finalStat = min.toString() + "," + avg.toString() + "," + max.toString()
        statsMap += (key -> finalStat)
      }
    }
    statsMap.toSeq.sortWith(_._2 > _._2).toMap
  }
}