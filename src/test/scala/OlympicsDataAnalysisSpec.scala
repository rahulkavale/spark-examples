package sparkExamples.olympicsData

import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.mutable._

import scala.collection.Seq

class OlympicsDataAnalysisSpec extends Specification {

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Olympics Data analysis"))
  "Olympics data analyzer" should {
    "aggregate countries for medals" in {

      val olympicsData = Seq("Yang Yilin,China,2008,Gymnastics,1,0,2,3", "Leisel Jones,Australia,2000,Swimming,0,2,0,2", "Go Gi-Hyeon,South Korea,2002,Short-Track Speed Skating,1,1,0,2",
        "Yang Yilin,China,2009,Gymnastics,1,1,2,4")

      val aggregations = OlympicsAnalysis.totalMedalsPerCountry(sc.parallelize(olympicsData), sc)

      aggregations.collect() mustEqual Array(("China",7), ("Australia",2), ("South Korea",2))

      sc.stop()
    }
  }
}