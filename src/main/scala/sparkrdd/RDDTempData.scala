package sparkrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._
import sparkrdd._

case class RDDTempData(id:String, date:Int, element:String, value:Int) // mFlag:String, qFlag:String, sFlag:String, obsTime:Int

object RDDTempData extends App {
  val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  var lines = sc.textFile("/home/gkilly/Projects/Trinity/Big-Data/in-class-code-gkilly/data/sparkrdd/ghcnd-stations.txt")
  val stationData = lines.map { line =>
    StationData.parseLine(line)
  }

  lines = sc.textFile("/home/gkilly/Projects/Trinity/Big-Data/in-class-code-gkilly/data/sparkrdd/2017.csv")
  val tempData2017 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }

  lines = sc.textFile("/home/gkilly/Projects/Trinity/Big-Data/in-class-code-gkilly/data/sparkrdd/2018.csv")
  val tempData2018 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }

  println
  println
  println
  println



  //#1
  val stationsInTexas = stationData.filter(_.name.substring(0,2) == "TX")
  println(stationsInTexas.count)

  //#2
  val activeStationsInTexas2017 = tempData2017.map(_.id).intersection(stationsInTexas.map(_.id))
  println(activeStationsInTexas2017.count)

  //#3
  val highestTemp2018 = tempData2018.filter(_.element == "TMAX").sortBy(-_.value).take(5)
  highestTemp2018.foreach(println)

  //#4
/*  val inactiveStations = stationData.count - tempData2017.map(_.id).distinct.count
    println(inactiveStations)*/
  val inactiveStations = stationsInTexas.map(_.id).subtract(tempData2017.map(_.id))
  println(inactiveStations.count)

  //#5
  //  val maxRainfallInTexas2017
  val randomStation = stationsInTexas.map(_.id).max
  println(randomStation)
  val maxRainfallInTexas2017 = tempData2017.filter(d => d.id == randomStation && d.element == "PRCP").sortBy(-_.value).take(5)
  maxRainfallInTexas2017.foreach(println)

  //#6
  val maxRainfallInIndia2017 = tempData2017.filter(d => d.id.substring(0, 2) == "IN" && d.element == "PRCP").sortBy(-_.value).take(5)
  maxRainfallInIndia2017.foreach(println)

  //#7
  val stationsInSanAntonio = stationData.filter(_.name.contains("TXSANANTONIO"))
  //val stationsNotInSanAntonio = stationData.filter(s => !(s.name.contains("TXSANANTONIO")))
  println(stationsInSanAntonio.count)

  //#8
  val activeStationsInSanAntonio2017 = tempData2017.map(_.id).intersection(stationsInSanAntonio.map(_.id))
  println(activeStationsInSanAntonio2017.count)

  //#9
  /*val largestDailyIncreases = tempData2017.filter(d => d.element == "TMAX" && d.element == "TMIN").keyBy(_.id).mapValues { idVals =>
    val differencesByDate = idVals.groupBy(_.date).mapValues { dateVals =>
      val maxi = dateVals.map(_.value).max
      val mini = dateVals.map(_.value).min
      maxi - mini
    }
    differencesByDate.sortBy(_._2)
  }
    val tempsFilter = tempData2017./.groupBy(_.id).mapValues { ids =>
      val o = ids.groupBy(_.date).mapValues { data =>
        data.map(_.value).max - data.map(_.value).min
      }
      o.toSeq.sortyBy(-_._2)
  }
  tempsFilter.take(10).foreach(println)
  val organizeByStation = tempsFilter.keyBy(_._1).reduceByKey(stationsNotInSanAntonio2017.keyBy(d => d))
  //val quickFilter= tempData2017.filter(d => activeStationsInSanAntonio2017.collect.contains(d.id)).take(5).foreach(println)
*/
//  val largestDailyIncreasesInSanAntonio = largestDailyIncreases.reduceByKey(inactiveStations.keyBy(_.id))
  //#11
  //Inefficent way of finding stations with decent data
  //stationData.filter(_.latitude.toInt == 74).take(10).foreach(println)
  //println(tempData2017.filter(d => d.id == "CA002401025" && d.element == "TMAX").count)
  //println(tempData2017.filter(d => d.id == "CA002401050" && d.element == "TMAX").count)

  var stationTemp = tempData2017.filter(d => d.id == "CA003072655" && d.element == "TMAX").sortBy(d => d.date)
  var time = 1 to 365 toArray//stationTemp.map(_.date-20170000).collect
  var rainfall = stationTemp.map(_.value/10.0).collect
  val chipewyanPlot = Plot.scatterPlot(time, rainfall, "Fort Chipewyan", "Day", "Temperature (degrees Celcius)", symbolSize = 8)
  SwingRenderer(chipewyanPlot, 800, 800, true)

  stationTemp = tempData2017.filter(d => d.id == "USW00013838" && d.element == "TMAX" && d.value != "-").sortBy(d => d.date)
  rainfall = stationTemp.map(_.value/10.0).collect
  val alPlot = Plot.scatterPlot(time, rainfall, "Mobile DWNT AP, AL", "Day", "Temperature (degrees Celcius)", symbolSize = 8)
  SwingRenderer(alPlot, 800, 800, true)

  stationTemp = tempData2017.filter(d => d.id == "CHM00059855" && d.element == "TMAX" && d.value != "-").sortBy(d => d.date)
  rainfall = stationTemp.map(_.value/10.0).collect
  val qioPlot = Plot.scatterPlot(time, rainfall, "QIONGHAI", "Day", "Temperature (degrees Celcius)", symbolSize = 8)
  SwingRenderer(qioPlot, 800, 800, true)

  stationTemp = tempData2017.filter(d => d.id == "BN000065335" && d.element == "TMAX" && d.value != "-").sortBy(d => d.date)
  rainfall = stationTemp.map(_.value/10.0).collect
  val savePlot = Plot.scatterPlot(time, rainfall, "SAVE", "Day", "Temperature (degrees Celcius)", symbolSize = 8)
  SwingRenderer(savePlot, 800, 800, true)

  stationTemp = tempData2017.filter(d => d.id == "NO000099710" && d.element == "TMAX" && d.value != "-").sortBy(d => d.date)
  rainfall = stationTemp.map(_.value/10.0).collect
  val bjorPlot = Plot.scatterPlot(time, rainfall, "BJOERNOEYA", "Day", "Temperature (degrees Celcius)", symbolSize = 8)
  SwingRenderer(bjorPlot, 800, 800, true)

  println
  println
  println
  println
}
