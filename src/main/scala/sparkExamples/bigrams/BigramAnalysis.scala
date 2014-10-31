package sparkExamples.bigrams

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import sparkExamples.bigrams.RDDImplicits.RichRDD

import scala.reflect.ClassTag

//the problem is from http://lintool.github.io/Cloud9/docs/exercises/bigrams.html
//scope for improvement by introducing domain models, using caching, hence WIP

object BigramAnalysis {
  def main(arg: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setMaster("yarn-client").setAppName("bigram analysis"))
    val file = sc.textFile("hdfs:///tmp/bible-shakes.nopunc.gz")

    val bigramsRDD = file.flatMap(Bigram.apply).filter(_.isValidBigram)


//     total number of bigrams
    bigramsRDD.count //    1578213

    bigramsRDD.cache()

//  How many unique bigrams are there?
    bigramsRDD.distinct.count    //  432310

    val bgOccrCount = bigramsRDD.countEachElement
    
    //    number of bigrams that appear only once:
    bgOccrCount.countWhere(a => a._2 == 1)//    296134

    //List the top ten most frequent bigrams and their counts.
    val bgOccrCountSorted = bgOccrCount.sortByDesc(a => a._2)
    bgOccrCountSorted.take(10)//    Array((of the,13037), (and the,7034), (the lord,7017), (in the,6738), (to the,3799), (i will,3470), (and he,3020), (shall be,3013), (all the,2714), (i have,2666))

    //  What fraction of all bigrams occurrences does the top ten bigrams account for? That is, what is the cumulative frequency of the top ten bigrams?
    val totalBgCount = bgOccrCount.values.reduce(_ + _)

    val topTenBgOccrCount = bgOccrCountSorted.take(10).map(a => a._2).reduce((a, b) => a + b)

    val fractionTopTenBigramOccurance = topTenBgOccrCount / totalBgCount.toFloat
    //  0.03327054

    val startingWordBigram =  bigramsRDD.map(bg => (bg.firstWord, bg))

//        val startgingWordAllBigrams = startingWordBigram.reduceByKey(_ + "$$$$$" + _).mapValues(a => a.split("$$$$$").toList)

    val startgingWordAllBigrams = startingWordBigram.aggregateByKey(BigramsWithSameStart("", List()))(
      (acc, bigram) => BigramsWithSameStart.apply(bigram),
      (bgs1, bgs2) => bgs1.merge(bgs2))
    
//    val startgingWordAllBigrams = startingWordBigram.reduceByKey((bg1, bg2) => Bigrams(b1, b2)).mapValues(a => a.split("$$$$$").toList)

    val startWordBGCount = startgingWordAllBigrams.map(a => (a._1, a._2.bigramsCount))

    val startWordBGAndBGCount = bgOccrCount.map(bg => (bg._1.firstWord, (bg._1, bg._2)))

    //    [(String, ((String, Int)), Int)]
    //   startWord - BG           - BG count    - bgs starting with word
    //    a._1     - a._2._1._1   - a._2._1._2  - a._2._2
    val startWordBGbGCountStartWordBgsCount = startWordBGAndBGCount.join(startWordBGCount).map(a => (a._1, a._2._1._1, a._2._1._2, a._2._2))
    startWordBGbGCountStartWordBgsCount.saveAsTextFile("hdfs:///tmp/bigramWordOccuranceCount.txt")

    //    bg - bg start word - reqlative freq

    //What are the five most frequent words following the word "light"? What is the frequency of observing each word?
    startWordBGbGCountStartWordBgsCount.cache()

    startWordBGbGCountStartWordBgsCount.filter(a => a._1.equals("light")).map(a => (a._1, a._2, a._4/a._3.toFloat)).sortBy(a => a._3).take(10)
//    Array((light,light and,0.01754386), (light,light of,0.018867925), (light,light to,0.05263158), (light,light in,0.06666667), (light,light on,0.06666667), (light,light is,0.07692308), (light,light upon,0.083333336), (light,light that,0.1), (light,light a,0.11111111), (light,light the,0.11111111))

//  Same question, except for the word "contain".
    startWordBGbGCountStartWordBgsCount.filter(a => a._1.equals("contain")).map(a => (a._1, a._2, a._4/a._3.toFloat)).sortBy(a => a._3).take(10)

//  Array((contain,contain the,0.33333334), (contain,contain a,0.33333334), (contain,contain thee,0.33333334), (contain,contain him,1.0), (contain,contain let,1.0), (contain,contain celestial,1.0), (contain,contain thyself,1.0), (contain,contain their,1.0), (contain,contain and,1.0), (contain,contain ourselves,1.0))

//  If there are a total of N words in your vocabulary, then there are a total of N2 possible values for F(Wn|Wn-1)—in theory, every word can follow every other word (including itself). What fraction of these values are non-zero? In other words, what proportion of all possible events are actually observed? To give a concrete example, let's say that following the word "happy", you only observe 100 different words in the text collection. This means that N-100 words are never seen after "happy" (perhaps the distribution of happiness is quite limited?).
    val allWords = file.flatMap(line => line.split(" "))
    val allDistinctWordsCount = allWords.distinct.count
    val totalPossibleCombinations = allDistinctWordsCount * allDistinctWordsCount

    val allDistinctBigrams = bigramsRDD.distinct.count

    val fractionOfBgsFoundOutOFTotalPossible = allDistinctBigrams / totalPossibleCombinations.toFloat
//  2.430579E-4

  }
}



