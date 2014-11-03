package sparkExamples.vehicleUsage

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object VehicleUsageAnalysis {
  def main(arg: Array[String]) = {

    val argsValid = arg.size == 4

    val master = if (argsValid) arg(0) else "yarn-client"

    val predictionDataFilePath =  if (argsValid) arg(1) else  "/Users/Rahul/projects/spark/dataset/vehicle-usage/servicingPrediction.csv"
    val vehiclesData =  if (argsValid) arg(2) else  "/Users/Rahul/projects/spark/dataset/vehicle-usage/vehicle.csv"
    val usageDataFilePath =  if (argsValid) arg(3) else  "/Users/Rahul/projects/spark/dataset/vehicle-usage/usage.csv"

    val sc = new SparkContext(
      new SparkConf()
        .setMaster(master)
        .setAppName("vehicle usage service prediction analysis")
    )

    val vehicles = sc.textFile(vehiclesData)
    val usages = sc.textFile(usageDataFilePath)

    val servicingPredictions = predictNextVehicleServicings(sc, vehicles, usages)

    servicingPredictions.saveAsTextFile(predictionDataFilePath)
  }

  def predictNextVehicleServicings(sc: SparkContext, vehiclesData: RDD[String], usagesData: RDD[String]): RDD[(Vehicle, List[DateTime])] = {
    val vehicles = vehiclesData.map(Vehicle.apply)
    val usages = usagesData.map(VehicleUsage.apply)

    val vehicleIDRDD = vehicles.map(v => (v.id, v))
    val usageIdRDD = usages.map(u => (u.vehicleId, u))

    val vehicleUsageRDD = vehicleIDRDD.join(usageIdRDD)

    vehicleUsageRDD.map { vu =>
      val vehicle = vu._2._1
      val usage = vu._2._2

      val servicingDays = vehicle.servicinInterval / usage.usageRate

      val numberOfPredictions = 5
      val nextServicingDates = (1 to numberOfPredictions).map(i => usage.captureDate.plusDays(servicingDays * i)).toList
      (vehicle, nextServicingDates)
    }
  }
}