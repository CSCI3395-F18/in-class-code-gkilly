package sparkrdd2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._
import sparkrdd._
import scala.math._

object AnswerSheet extends App {
  val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  var lines = sc.textFile("/home/gkilly/Projects/Trinity/Big-Data/in-class-code-gkilly/data/sparkrdd/2017.csv")
  val tempData2017 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }

  lines = sc.textFile("/home/gkilly/Projects/Trinity/Big-Data/in-class-code-gkilly/data/sparkrdd2/1897.csv")
  val tempData1897 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }

  lines = sc.textFile("/home/gkilly/Projects/Trinity/Big-Data/in-class-code-gkilly/data/sparkrdd/ghcnd-stations.txt")
  val stationData = lines.map { line =>
    StationData.parseLine(line)
  }

/*
//Number 1
val check = tempData2017.filter(td => (td.element == "TMIN" || td.element == "TMAX") && td.date == 20170719 && td.id == "USS0047P03S" ) //making sure values were calculated correctly
println(check.count)
check.foreach(println)

val diffByIDAndDate = tempData2017.filter(td => td.element == "TMIN" || td.element == "TMAX").map(td => ((td.id, td.date), td.value)).reduceByKey((acc, v) => (acc - v)).map(d => (d._1, abs(d._2)))
val highestDiffDay2017 = diffByIDAndDate.map(_.swap).sortByKey(false)
highestDiffDay2017.take(5).foreach(println)

//Number 2 TODO USE AVERAGES INSTEAD SUM
//val highestDiffStation2017 = diffByIDAndDate.map(d => (d._1._1, d._2)).reduceByKey((acc, v) => (acc + v)).map(_.swap).sortByKey(false)
val highestDiffStation2017A = diffByIDAndDate.map(d => (d._1._1, d._2)).aggregateByKey(0.0 -> 0)({ case ((sum, cnt), v) =>
  (sum+v, cnt+1)
}, { case ((s1, c1), (s2, c2)) => (s1+s2, c1+c2) })
val highestDiffStation2017B = highestDiffStation2017A.map { case (id, (s,c)) => (s/c, id) }.sortByKey(false)
highestDiffStation2017B.take(5).foreach(println)

//Number 3
val stdDevOfTMax = tempData2017.filter(sd => sd.element == "TMAX").map(_.value.toDouble).stdev()
println(stdDevOfTMax)
val stdDevOfTMin = tempData2017.filter(sd => sd.element == "TMIN").map(_.value.toDouble).stdev()
println(stdDevOfTMin)

//Number 4
val activeStationsInBothYears = tempData2017.map(_.id).intersection(tempData1897.map(_.id)).count
println(activeStationsInBothYears)
*/
//Number 5
val lowLatGroup = tempData2017.map(td => (td.id, td)).subtractByKey(stationData.filter(_.latitude < 35).map(sd => (sd.id, sd.id))).map(_._2)
//val midLatGroup = tempData2017.map(td => (td.id, td)).subtractByKey(stationData.filter(sd => sd.latitude > 35 && sd.latitude <= 41).map(sd => (sd.id, sd.id))).map(_._2)
//val highLatGroup = tempData2017.map(td => (td.id, td)).subtractByKey(stationData.filter(_.latitude > 42).map(sd => (sd.id, sd.id))).map(_._2)
//5a
lowLatGroup.take(5).foreach(println)
//midLatGroup.take(5).foreach(println)
//highLatGroup.take(5).foreach(println)

val stdTMAXLow = lowLatGroup.filter(_.element == "TMAX").map(_.value.toDouble).stdev()
//val stdTMAXMid = midLatGroup.filter(_.element == "TMAX").map(_.value.toDouble).stdev()
//val stdTMAXHigh = highLatGroup.filter(_.element == "TMAX").map(_.value.toDouble).stdev()

val stdTMINLow = lowLatGroup.filter(_.element == "TMIN").map(_.value.toDouble).stdev()
//val stdTMINMid = midLatGroup.filter(_.element == "TMIN").map(_.value.toDouble).stdev()
//val stdTMINHigh = highLatGroup.filter(_.element == "TMIN").map(_.value.toDouble).stdev()

println("Low: TMAX deviation = " + stdTMAXLow + " TMIN deviation = " + stdTMINLow)
//println("Mid: TMAX deviation = " + stdTMAXMid + " TMIN deviation = " + stdTMINMid)
//println("High: TMAX deviation = " + stdTMAXHigh + " TMIN deviation = " + stdTMINHigh)

val stdAvgDailyLow = lowLatGroup.filter(llg => llg.element == "TMAX" || llg.element == "TMIN").map(llg => ((llg.id, llg.date), llg.value)).reduceByKey((acc, v) => (acc + v)/2).map(_._2).stdev()


}
