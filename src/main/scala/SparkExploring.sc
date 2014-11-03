import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.joda.time.DateTime

import org.apache.spark.{SparkConf, SparkContext}
import sparkExamples.wrappers.RDDImplicits.RichRDD
val input = """1: if you prick us do we not bleed
2: if you tickle us do we not laugh
3: if you poison us do we not die and
4: if you wrong us shall we not revenge"""
val sc = new SparkContext(
  new SparkConf()
    .setMaster("local")
    .setAppName("bigram analysis")
)
//val parsedInput = input.split("\n")
//val lines = sc.parallelize(parsedInput)
////val mapped = lines.map(a => (a.split(" "), a))
////val wordLine = mapped.flatMap(a1 => a1._1.map(b1 => (b1, a1._2)))
//val wordToLine = lines.explode(line => line.split(" ").toList)
//
//
//println(wordToLine.collect())
//


