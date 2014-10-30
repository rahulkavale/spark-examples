package spike.bigrams

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

//the problem is from http://lintool.github.io/Cloud9/docs/exercises/bigrams.html
//scope for improvement by introducing domain models, using caching, hence WIP
object BigramAnalysis {
  def main(arg: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setMaster("yarn-client").setAppName("bigram analysis"))
    val file = sc.textFile("hdfs:///tmp/bible-shakes.nopunc.gz")
    val bigramsRDD = file.flatMap(a => getBigrams(a)).filter(a => a.trim.count(_ == ' ') == 1 )

    //  How many unique bigrams are there?
    bigramsRDD.distinct.count
    //  432310

    //     total number of bigrams
    bigramsRDD.count
    //    1578213

    bigramsRDD.cache()

    val bigramCount = bigramsRDD.map(bg => (bg, 1)).reduceByKey((c1, c2) => c1 + c2)
    //    number of bigrams that appear only once:
    bigramCount.filter(a => a._2 == 1).count
    //    296134

    //List the top ten most frequent bigrams and their counts.
    val bigramsSortedByoccuranceCount = bigramCount.sortBy(a => -a._2)
    bigramsSortedByoccuranceCount.take(10)
    //    Array((of the,13037), (and the,7034), (the lord,7017), (in the,6738), (to the,3799), (i will,3470), (and he,3020), (shall be,3013), (all the,2714), (i have,2666))

    //  What fraction of all bigrams occurrences does the top ten bigrams account for? That is, what is the cumulative frequency of the top ten bigrams?
    val totalBigramCount = bigramCount.map(a => a._2).reduce(_ + _)
    val totalOccurancesOfTopTenBigrams = bigramsSortedByoccuranceCount.take(10).map(a => a._2).reduce((a, b) => a + b)
    val fractionTopTenBigramOccurance = totalOccurancesOfTopTenBigrams / totalBigramCount.toFloat
    //  0.03327054

    val startingWordBigram =  bigramsRDD.map(bg => (bg.split(" ")(0), bg))

    val startgingWordAllBigrams = startingWordBigram.reduceByKey(_ + "$$$$$" + _).mapValues(a => a.split("$$$$$").toList)

    val startWordBGCount = startgingWordAllBigrams.map(a => (a._1, a._2.size))

    val startWordBGAndBGCount = bigramCount.map(bg => (bg._1.split(" ")(0), (bg._1, bg._2)))

    //    [(String, ((String, Int)), Int)]
    //   startWord - BG           - BG count    - bgs starting with word
    //    a._1     - a._2._1._1   - a._2._1._2  - a._2._2
    val startWordBGbGCountStartWordBgsCount = startWordBGAndBGCount.join(startWordBGCount).map(a => (a._1, a._2._1._1, a._2._1._2, a._2._2))
    startWordBGbGCountStartWordBgsCount.saveAsTextFile("hdfs:///tmp/bigramWordOccuranceCount.txt")

    //    bg - bg start word - reqlative freq
    val bigramRelativeFrequencies = startWordBGbGCountStartWordBgsCount.map(a => (a._2, a._1, a._4.toFloat/a._3.toFloat))

    //What are the five most frequent words following the word "light"? What is the frequency of observing each word?
    bigramRelativeFrequencies.filter(a => a._2.equals("light")).sortBy(a => - a._3).take(5)
    //    Array[(String, String, Float)] = Array((light what,light,1.0), (light were,light,1.0), (light murder,light,1.0), (light joy,light,1.0), (light gentlemen,light,1.0))


    //    Same question, except for the word "contain".
    bigramRelativeFrequencies.filter(a => a._2.equals("contain")).sortBy(a => - a._3).take(5)
    //    Array[(String, String, Float)] = Array((contain him,contain,1.0), (contain let,contain,1.0), (contain celestial,contain,1.0), (contain thyself,contain,1.0), (contain their,contain,1.0))

    //    If there are a total of N words in your vocabulary, then there are a total of N2 possible values for F(Wn|Wn-1)—in theory, every word can follow every other word (including itself). What fraction of these values are non-zero? In other words, what proportion of all possible events are actually observed? To give a concrete example, let's say that following the word "happy", you only observe 100 different words in the text collection. This means that N-100 words are never seen after "happy" (perhaps the distribution of happiness is quite limited?).
    val allWords = file.flatMap(line => line.split(" "))
    val allWordsCount = allWords.count
    val totalPossibleCombinations = allWordsCount * allWordsCount

    val allBigramsForThisWord = startWordBGCount.map(a => (a._1, (totalPossibleCombinations - a._2)/totalPossibleCombinations.toFloat))

  }
  def getBigrams(input: String): List[String] = input.split(" ").sliding(2).toList.map(ws => ws.mkString(" "))
}
