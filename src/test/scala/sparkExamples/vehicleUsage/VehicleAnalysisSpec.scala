package sparkExamples.vehicleUsage

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import org.specs2.mutable._

import scala.collection.Seq

class VehicleAnalysisSpec extends Specification {
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Vehicle Servicing Prediction test"))
  "Vehicle Servicing Analyser" should {
    "predict next servicing dates" in {

      val usageData = Seq("1234,100,1000,2014-01-01",
                        "1235,500,5500,2014-01-01",
                        "1231,1000,1200,2014-01-01")

      val vehicleData = Seq("1234,i10,UP,10000",
                          "1235,i20,NEWDELHI,20000",
                          "1231,Polo,MH,30000")
      

      val predictions = VehicleUsageAnalysis
        .predictNextVehicleServicings(
          sc,
          sc.parallelize(vehicleData),
          sc.parallelize(usageData))


      val vehicle1 = Vehicle("1234,i10,UP,10000")

      val dateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd");

      val dt = dateTimeFormat.parseDateTime("2014-04-11")
      val dt1 = dateTimeFormat.parseDateTime("2014-07-20")
      val dt2 = dateTimeFormat.parseDateTime("2014-10-28")
      val dt3 = dateTimeFormat.parseDateTime("2015-02-05")
      val dt4 = dateTimeFormat.parseDateTime("2015-05-16")

      predictions.lookup(vehicle1).head mustEqual List(dt, dt1, dt2, dt3, dt4)

      sc.stop()
    }
  }
}