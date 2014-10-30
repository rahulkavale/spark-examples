package spike.movieAnalytics

case class Rating(userId: Int, movieId: Int, rating: Int, timestamp: Long)
object Rating{
  //UserID::MovieID::Rating::Timestamp
  //1::1193::5::978300760
  def apply(input: String): Rating = {
    val ratingInputParts = input.split("::")
    val userId = ratingInputParts(0).toInt
    val movieId = ratingInputParts(1).toInt
    val rating = ratingInputParts(2).toInt
    val timestamp = ratingInputParts(3).toLong
    if(!(
      userId >= 1 && userId <= 6040 &&
        movieId >= 1 && movieId <= 3952 &&
        rating >=1 && rating <= 5
      ))
      throw new IllegalArgumentException("Invalid rating" + userId + " " + movieId +  " " + rating +  " " + timestamp)
    new Rating(userId, movieId, rating, timestamp)
  }

  def inc(x: Int): Int = x + 1
  def dec(x: Int): Int = x - 1
}
