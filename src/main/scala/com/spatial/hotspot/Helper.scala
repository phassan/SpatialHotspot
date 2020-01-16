package com.spatial.hotspot

import java.text.SimpleDateFormat
import java.util.Calendar;
import java.util.Date;
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{ FileSystem, Path }
import java.io._

object Helper extends java.io.Serializable {

  var lat_start = 4050
  //var lat_start = 40
  var lat_end = 4090
  //var lat_end = 43
  var lon_start = -7425
  //var lon_start = 70
  var lon_end = -7370
  //var lon_end = 73
  var cell_length = 1
  //var cell_length = 1
  var no_of_days = 31
  //var no_of_days = 3

  def calculateHotspot(sc: SparkContext, inputFile: String, outputPath: String): Unit = {

    try {

      var start_time = Calendar.getInstance().getTime()
      // Step 1: Reading CSV file and saving the data in RDD[String].
      val inputCsvFile = sc.textFile(inputFile)
      // Step 2: Remove header from the data.
      val header = inputCsvFile.first()
      val inputRows = inputCsvFile.filter { x => x != header }
      //Step 3:
      var inputRowsLst = inputRows.map(line => {
        //        var lineArr = line.split(";")
        var lineArr = line.split(",")
        // For all rows in data, rowConvert() converts pickup date time, longitude and latitude to cell coordinate 
        // with corresponding number of pickup occurrences of that coordinate.
        rowConvert(lineArr(1), lineArr(5), lineArr(6))
      })

      // Step 4: Filter out the data that does not have any pickup occurrence. 
      val inputRowsLstFinal = inputRowsLst.filter { case (x, y) => x != "" }

      // Step 5: Group pickup occurrences with their respective cell coordinates.
      val inputRowsGpBy = inputRowsLstFinal.groupByKey()

      //Step 6: Calculate the total number occurrences for each cell coordinate.
      val inputRowsMap = inputRowsGpBy.map { case (x, y) => (x, y.size) }

      var total_cell_no = ((lat_end - lat_start) / cell_length) * ((lon_end - lon_start) / cell_length) * no_of_days

      // Step 7: Calculate Mean and Standard Deviation.
      var x_bar = inputRowsMap.map((_._2)).sum() / total_cell_no
      var tmp_x_bar = inputRowsMap.map(x => Math.pow(x._2, 2)).sum()
      var S = Math.sqrt((tmp_x_bar / total_cell_no) - Math.pow(x_bar, 2));

      // Step 8: Collect data as Map.
      var inputRowsMapVal = inputRowsMap.collectAsMap()
      //      inputRowsMapVal.foreach(x => {
      //        println(x._1 + " --> " + x._2)
      //      })

      // Step 9: Create cell coordinates for the whole cube.
      var cube = for (day <- 1 to no_of_days by 1; lonVal <- lon_start to lon_end - cell_length by cell_length; latVal <- lat_start to lat_end - cell_length by cell_length) yield {

        var cellKey_y = ((latVal - lat_start) / cell_length).toInt
        var cellKey_x = ((lonVal - lon_start) / cell_length).toInt
        var cellKey = (day - 1) + " " + cellKey_x + " " + cellKey_y
        (cellKey -> 0)
      }

      //      date X Y (limit 0 0 0 to 30 54 39) 
      //      cube.foreach(x => {
      //        println(x._1 + " --> " + x._2)
      //      })
      
      // Step 10: Assign the total pickup occurrence in each grid cell.
      var finalInputRowsRDDVal = sc.parallelize(cube).map { m =>
        (m._1 ->
          {
            if (inputRowsMapVal.get(m._1) != None) {
              inputRowsMapVal.get(m._1).get
            } else { 0 }
          })
      }

      //      finalInputRowsRDDVal.filter(x => x._2 != 0).foreach(x => {
      //        println(x._1 + " --> " + x._2)
      //      })
      
      // Step 11: Collect cell coordinates with their total pickup occurrences as map.
      var finalInputRowsMapVal = finalInputRowsRDDVal.collectAsMap()

      //      finalInputRowsMapVal.foreach(x => {
      //        println(x._1 + " --> " + x._2)
      //      })
      
      // Step 12: Calculate zscore for each cell coordinate of the cube.
      var inputRowsMap_Zscore = finalInputRowsRDDVal.map(x => (calculateOutputKey(x._1), calculateZscore(x._1, finalInputRowsMapVal, total_cell_no, x_bar, S)))

      // Step 13: Sort the result in descending order based on zscore.
      val result = inputRowsMap_Zscore.sortBy(r => (-r._2))
      
      // Step 14: Return top 50 records and saving the final result in a single output file.
      val finalResult = result.take(50)

      sc.parallelize(finalResult).map(w => w._1 + "," + w._2).coalesce(1).saveAsTextFile(outputPath + "result.txt");

      var end_time = Calendar.getInstance().getTime()
      println("Total TIME:- " + end_time + " -- " + start_time)
    } catch {
      case e: Exception => e.fillInStackTrace()
    }

  }

  def rowConvert(line: (String, String, String)): (String, Int) = {

    var date = line._1.split(" ")(0)
    //var dateFormat = "dd/MM/yy"
    var dateFormat = "yyyy-MM-dd"
    val inputFormat = new SimpleDateFormat(dateFormat)
    val outputFormat = new SimpleDateFormat("dd")
    val formattedDate = outputFormat.format(inputFormat.parse(date))

    var finalCount = 0
    var csv_row_lon = line._2.toDouble * 100
    var csv_row_lat = line._3.toDouble * 100
    //    var csv_row_lon = line._2.replace("(", "").replace(")", "").split(",")(0).toDouble * 100
    //    var csv_row_lat = line._2.replace("(", "").replace(")", "").split(",")(1).toDouble * 100

    if (csv_row_lon >= lon_start && csv_row_lon <= lon_end && csv_row_lat >= lat_start && csv_row_lat <= lat_end) {

      var cellKey_x = ((csv_row_lon - lon_start) / cell_length).toInt
      var cellKey_y = ((csv_row_lat - lat_start) / cell_length).toInt
      var cellKey = (formattedDate.toInt - 1) + " " + cellKey_x + " " + cellKey_y
      finalCount = finalCount + 1

      //      println(csv_row_lon +"---->"+ csv_row_lat +"---->"+cellKey + "---->" + finalCount)

      return (cellKey, finalCount)
    }

    return ("", finalCount)

  }

  //  The Getis-Ord statistic approach considers each item within
  //  the context of its neighbors. An item with a high value might
  //  be important but may not be statistically significant hot-spot.
  //  An item is considered significant if it has a high value and is
  //  surrounded by other items with high values as well.
  //  We employ Getis-Ord statistic and generate z-score for each cell in the cube.

  //  z_score is the number of standard deviations that it is above or below the population mean
  def calculateZscore(key_i: String, entireMap: scala.collection.Map[String, Int], total_cell_no: Double, x_bar: Double, S: Double): Double = {
    var z_score = 0.0
    var neighbor: List[(String, Int)] = List()
    var keyValArr = key_i.split(" ");
    var constants: List[Int] = List(-1, 0, 1)
    for (constant_i <- constants) {
      var key_frst_part = keyValArr(0).toInt + constant_i
      for (constant_j <- constants) {
        var key1 = key_frst_part + " " + (keyValArr(1).toInt + constant_j) + " " + (keyValArr(2).toInt - 1);
        var key2 = key_frst_part + " " + (keyValArr(1).toInt + constant_j) + " " + (keyValArr(2).toInt + 0);
        var key3 = key_frst_part + " " + (keyValArr(1).toInt + constant_j) + " " + (keyValArr(2).toInt + 1);

        //       println(key1 + "--->" + key2 + "--->" + key3)
        //       date X Y   1 0 0 to 1 54 39 
        //       1 33 30
        //       1 33 29--->1 33 30--->1 33 31

        var val1 = { if (entireMap.get(key1) != None) entireMap.get(key1).get else -1 }
        if (!key_i.equalsIgnoreCase(key1) && val1 != -1) {
          neighbor = neighbor :+ (key1, val1)
        }
        var val2 = { if (entireMap.get(key2) != None) entireMap.get(key2).get else -1 }
        if (!key_i.equalsIgnoreCase(key2) && val2 != -1) {
          neighbor = neighbor :+ (key2, val2)
        }
        var val3 = { if (entireMap.get(key3) != None) entireMap.get(key3).get else -1 }
        if (!key_i.equalsIgnoreCase(key3) && val3 != -1) {
          neighbor = neighbor :+ (key3, val3)
        }

      }

    }

    var val_self = { if (entireMap.get(key_i) != None) entireMap.get(key_i).get else -1 }
    neighbor = neighbor :+ (key_i, val_self)
    var frst_term = neighbor.map((_._2)).sum
    var second_term = neighbor.size
    var third_term = neighbor.size
    var z_numerator = (frst_term - x_bar * second_term)
    var z_denomenator = (S * Math.sqrt((total_cell_no * (third_term) - Math.pow(second_term, 2)) / (total_cell_no - 1)))
    z_score = z_numerator / z_denomenator

    return z_score

  }

  def calculateOutputKey(in: String): String = {

    var keyData = in.split(" ")
    var dayVal = keyData(0).toInt
    var keyX = keyData(1).toInt + lon_start
    var keyY = keyData(2).toInt + lat_start

    var cellKey = keyY + "," + keyX + "," + dayVal

    return cellKey
  }

  def removeDirInHDFS(sc: SparkContext, dirPath: String) = {

    val hconf = sc.hadoopConfiguration
    val dfs = FileSystem.get(hconf)

    var path = new Path(dirPath)
    if (dfs.exists(path)) {
      dfs.delete(path, true)
    }
  }

  def createDirInHDFS(sc: SparkContext, dirPath: String) = {

    val hconf = sc.hadoopConfiguration
    val dfs = FileSystem.get(hconf)

    var path = new Path(dirPath)
    if (!dfs.exists(path)) {
      dfs.mkdirs(path)
    }
  }

}
