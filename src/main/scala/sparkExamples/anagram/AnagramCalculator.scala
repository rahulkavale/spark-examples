package sparkExamples.anagram

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


object AnagramCalculator2 {
  def calculateAnagram(source: String, dest: String) = {
  val sc = new SparkContext(new SparkConf().setAppName("Anagram Calculator").setMaster("local"))
  val input = sc.textFile(s"/Users/Rahul/projects/spark/dataset/mword10/$source")
  val wordToSortedChars = input.map(word => (word.toCharArray.toList.sorted.mkString, word.trim))
  val wordsGrouped = wordToSortedChars.reduceByKey{(a, b) =>
    if(a.contains(b)) a
    else a + " " + b
  }
  val wordsWithMoreThanOneAnagram = wordsGrouped.filter(a => a._2.count(_ == ' ') > 1)

  wordsWithMoreThanOneAnagram.saveAsTextFile("/Users/Rahul/projects/spark/dataset/mword10/" + dest)
  }
}


object AnagramCalculator3 {

  def calculateAnagram(source: String, dest: String) = {
    val sc = new SparkContext(new SparkConf()
      .setAppName("Anagram Calculator")
      .setMaster("local")
    )
    val input = sc.textFile(source)
    val wordToSortedChars = input.map(word => (word.toCharArray.toList.sorted.mkString, Set(word.trim)))
    val uniqueWordsForGivenChars = wordToSortedChars.reduceByKey{(a, b) =>
      a.union(b)
    }
    val wordsWithMoreThanOneAnagram = uniqueWordsForGivenChars.filter(a => a._2.size > 1)

    wordsWithMoreThanOneAnagram.saveAsTextFile(dest)
  }
}
object AnagramCalculator1 {

  case class WordAnagrams(word: String, anagrams: Set[String]){
    def addAnagrams(wordAnagram: WordAnagrams): WordAnagrams = {
      WordAnagrams(word, anagrams.union(wordAnagram.anagrams))
    }
    def containsMoreThanOneAnagram: Boolean = {
      anagrams.size > 1
    }
  }
}
object AnagramCalculator4 {
  def calculateAnagram(sourceFilePath: String, destinationFilePath: String) = {

    val sc = new SparkContext(new SparkConf()
      .setAppName("Anagram Calculator")
      .setMaster("local"))

    val words = sc.textFile(sourceFilePath).flatMap(a => a.split(" "))

    val wordToSortedChars = words.map { word =>
      (word.toCharArray.toList.sorted.mkString, Set(word.trim))
    }

    val wordsGrouped = wordToSortedChars.reduceByKey{ (a, b) =>
      a.union(b)
    }

    val wordsWithMoreThanOneAnagram = wordsGrouped.filter{wordAnagrams => wordAnagrams._2.size > 1}

    wordsWithMoreThanOneAnagram.saveAsTextFile(destinationFilePath)
  }
}
