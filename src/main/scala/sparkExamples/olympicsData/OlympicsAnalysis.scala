package sparkExamples.olympicsData

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//data from https://github.com/mortardata/mortar-examples/blob/master/data/OlympicAthletes.csv
case class Record(name: String, country: String, year: Int, category: String, gold: Int, silver: Int, bronze: Int, total: Int)

object OlympicsAnalysis {

  def main(arg: Array[String]) = {

    val argsValid = arg.size == 3

    val master = if (argsValid) arg(0) else "yarn-client"

    val inputFilePath = if (argsValid) arg(1) else "hdfs:///tmp/olympics-data.csv"

    val outputFilePath = if (argsValid) arg(1) else "hdfs:///tmp/olympics-aggregations"

    val sc = new SparkContext(
      new SparkConf()
        .setMaster(master)
        .setAppName("olympics data analysis")
    )
    val file = sc.textFile(inputFilePath)

    //      TODO need to have generic logic, or implementation of discard lines/records by index
    val filteredRecords = file.filter(line => !(line.equals("") || line.startsWith("Athlete")))

    val medalsPerCountry: RDD[(String, Int)] = totalMedalsPerCountry(filteredRecords, sc)

    medalsPerCountry.saveAsTextFile(outputFilePath)
  }

  def parse(input: String): Record = {
    val split = input.split(",")
    Record(split(0), split(1), split(2).toInt, split(3), split(4).toInt, split(5).toInt, split(6).toInt, split(7).toInt)
  }

  def totalMedalsPerCountry(input: RDD[String], sc: SparkContext): RDD[(String, Int)] = {

    val parsedRecords = input.map(parse)

    parsedRecords.map(record => (record.country, record.total)).reduceByKey(_ + _).sortBy { case (country, total) => -total}
  }
}
