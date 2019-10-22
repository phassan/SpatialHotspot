package com.spatial.hotspot

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j._

object Driver {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setAppName("Spatial-Hotspot")
//      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    Helper.removeDirInHDFS(sc, args(1))
    Helper.createDirInHDFS(sc, args(1))
    Helper.calculateHotspot(sc, args(0), args(1))

  }

}