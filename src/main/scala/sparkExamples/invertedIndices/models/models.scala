package sparkExamples.invertedIndices.models

object models {

  case class Line(lineNumber: Long, text: String) {
    def words = text.split(" ").toList

    def occuranceCount(word: Word) = words.count(_.equals(word.wordText))
  }

  case class Word(wordText: String)

  case class OccrCount[T](count: Int){
    override def toString = count.toString
  }

  case class InvertedIndex(word: Word, count: OccrCount[Word], occurances: Set[(Long, Int)]) {
    def histogram = {
      val histogram = occurances
        .map(a => (a._2, a._1))
        .groupBy(a => a._1)
        .map(a => (a._1, a._2.size))
      (word, histogram)
    }


    override def toString = word.wordText + "\t" + count + "\t" + occurances.mkString(",")
  }

}