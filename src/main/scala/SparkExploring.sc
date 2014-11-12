import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext.{numericRDDToDoubleRDDFunctions, rddToPairRDDFunctions}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import sparkExamples.wrappers.RDDImplicits.RichRDD

//val input = """1: if you prick us do we not bleed
//2: if you tickle us do we not laugh
//3: if you poison us do we not die and
//4: if you wrong us shall we not revenge"""
//
//val parsedInput = input.split("\n")
//val lines = sc.parallelize(parsedInput)
//val mapped = lines.map(a => (a.split(" "), a))

val sc = new SparkContext(
  new SparkConf()
    .setMaster("local")
    .setAppName("bigram analysis")
)

//val input  = sc.textFile("hdfs:///tmp/bible-shakes.nopunc.gz")
val input1  = sc.textFile("/Users/Rahul/Downloads/bible+shakes.nopunc.gz").take(5)
val input  = sc.parallelize(input1)

case class Line(lineNumber: Long, text: String){
  def words = text.split(" ").toList

  def occuranceCount(word: Word) = words.count(_.equals(word.wordText))
}

case class Word(wordText: String)

case class OccrCount[T](count: Int)

val lineWithLineNumbers = input.zipWithIndex().map(a => Line(a._2, a._1))
//val wordLine = lineWithLineNumbers.map(a => (a.split(" "), a)).flatMap(a1 => a1._1.map(b1 => (b1, a1._2)))

val wordToLine = lineWithLineNumbers.explode(line => line.words)


println(wordToLine.collect())

val wordLines = wordToLine.map(a => (Word(a._1), List(a._2))).reduceByKey(_ ++ _)

val wordCountLine = wordLines.map(a => (a._1, OccrCount[Word](a._2.size), a._2))

val wordCountLinePerLineCount = wordCountLine.map(a => (a._1, a._2, a._3.map(line => (line.lineNumber, line.occuranceCount(a._1)))))

//wordCountLinePerLineCount.collect().sortBy(a => a._1)

//                       term     -    df      -   (docno, tf)
case class InvertedIndex(word: Word, count: OccrCount[Word], occurances: List[(Long, Int)])

val invertedIndices = wordCountLinePerLineCount.map(a => InvertedIndex(a._1, a._2, a._3))

invertedIndices.cache()


invertedIndices.filter(invertedIndex => invertedIndex.word.equals(Word("starcrossd"))).collect

//sc.wholeTextFiles()
//wordLines.stats()


//sc.textFile("", classOf[TextInputFormat], )
//
//sc.textFile()
//hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
//  minPartitions).map(pair => pair._2.toString).setName(path)

//XmlInputFormat

//LongWritable, Text, X
//MLInputFormat
//sc.hadoopFile("", classOf[XmlInputFormat], LongWritable, Text)

//new SQLContext(sc).jsonFile()