package sparkExamples.bigrams.models


case class Bigram(firstWord: String, secondWord: String) {
  //  def merge(bigram: Bigram): Bigram = Bigram(firstWord, secondWord + "$$$$$$$$$$" + secondWord)

  def isValidBigram: Boolean = !firstWord.equals(" ") && !secondWord.equals(" ")
}

case class BigramsWithSameStart(firstWord: String, bigrams: List[String]) {
  def bigramsCount = bigrams.size

  def merge(bg2: BigramsWithSameStart): BigramsWithSameStart = {
    new BigramsWithSameStart(firstWord, bigrams ++ bg2.bigrams)
  }
}

object BigramsWithSameStart {
  def apply(bigram: Bigram) = {
    new BigramsWithSameStart(bigram.firstWord, List(bigram.secondWord))
  }
}

object Bigram {
  def apply(input: String): List[Bigram] = {
    val splitWords = input.split(" ").sliding(2).toList
    splitWords.map(words => new Bigram(words(0), words(1)))
  }

}