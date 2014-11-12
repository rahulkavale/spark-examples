import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import sparkExamples.invertedIndices.models.models._
import sparkExamples.wrappers.RDDImplicits.RichRDD

val input = """if you prick us do we not bleed
if you tickle us do you we not laugh do"""

val sc = new SparkContext(
  new SparkConf()
    .setMaster("local")
    .setAppName("bigram analysis")
)

val parsedInput = input.split("\n")
val lines = sc.parallelize(parsedInput)

//val input  = sc.textFile("hdfs:///tmp/bible-shakes.nopunc.gz")
//val input1  = sc.textFile("/Users/Rahul/Downloads/bible+shakes.nopunc.gz").take(5)
//val input  = sc.parallelize(input1)

//case class Line(lineNumber: Long, text: String){
//  def words = text.split(" ").toList
//
//  def occuranceCount(word: Word) = words.count(_.equals(word.wordText))
//}
//
//case class Word(wordText: String)
//
//case class OccrCount[T](count: Int)

val lineWithLineNumbers = lines.zipWithIndex().map(a => Line(a._2, a._1))
//val wordLine = lineWithLineNumbers.map(a => (a.split(" "), a)).flatMap(a1 => a1._1.map(b1 => (b1, a1._2)))

val wordToLine = lineWithLineNumbers.explode(line => line.words)


println(wordToLine.collect())

val wordLines = wordToLine.map(a => (Word(a._1), Set(a._2))).reduceByKey(_ ++ _)

val wordCountLine = wordLines.map(a => (a._1, OccrCount[Word](a._2.size), a._2))

val wordCountLinePerLineCount = wordCountLine.map(a => ((a._1, a._2), a._3.map(line => (line.lineNumber, line.occuranceCount(a._1)))))

//wordCountLinePerLineCount.reduceByKey(a => )

//                       term     -    df      -   (docno, tf)
//case class InvertedIndex(word: Word, count: OccrCount[Word], occurances: List[(Long, Int)])

val invertedIndices = wordCountLinePerLineCount.map(a => InvertedIndex(a._1._1, a._1._2, a._2))

invertedIndices.foreach(a => println(a.histogram))

invertedIndices.cache()

invertedIndices.collect

invertedIndices.filter(invertedIndex => invertedIndex.word.equals(Word("if"))).first


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