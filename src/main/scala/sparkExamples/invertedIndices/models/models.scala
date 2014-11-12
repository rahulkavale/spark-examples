package sparkExamples.invertedIndices.models

object models {

  case class Line(lineNumber: Long, text: String) {
    def words = text.split(" ").toList

    def occuranceCount(word: Word) = words.count(_.equals(word.wordText))
  }

  case class Word(wordText: String)

  case class OccrCount[T](count: Int)

  case class InvertedIndex(word: Word, count: OccrCount[Word], occurances: List[(Long, Int)]) {
    def histogram =
      occurances
      .map(a => (a._2, 1))
      .groupBy(a => a._1)
      .map(a => (a._1, a._2.size))
  }

}
