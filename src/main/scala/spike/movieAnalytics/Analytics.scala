package spike.movieAnalytics

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object Analytics {
  def main(arg: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setMaster("yarn-client").setAppName("movie analytics example"))
    val moviesInput = sc.textFile("/Users/Rahul/Downloads/spark-training/data/movielens/medium/movies.dat")
    val movies =  moviesInput.map{input =>
      val movie = MovieInfo.apply(input)
      (movie.movieID, movie)
    }

    val ratingsInput = sc.textFile("/Users/Rahul/Downloads/spark-training/data/movielens/medium/ratings.dat")
    val ratings = ratingsInput.map(Rating.apply)

    val usersInput = sc.textFile("/Users/Rahul/Downloads/spark-training/data/movielens/medium/users.dat")
    val users = usersInput.map{input =>
      val user = User.apply(input)
      (user.userID, user)
    }

    val rdd1 = sc.parallelize(1 to 100)
    val async = rdd1.countAsync()
//    Users
//    UserID::Gender::Age::Occupation::Zip-code
//    "1::Toy Story (1995)::Animation|Children's|Comedy"

//    Ratings
//    UserID::MovieID::Rating::Timestamp
//    1::1193::5::978300760
//    UserID::MovieID::Rating::Timestamp
//    1::1194::5::978300760

//    Movies
//    MovieID::Title::Genres
//    1::Toy Story (1995)::Animation|Children's|Comedy

//    most fav movie of a user, given highest rating

//    number of movies a user has rated sorted bigramsSortedByoccuranceCount ratings count
    ratings.map(a => a.userId).countByValue.toSeq.sortBy(- _._2)

//    movie having most number of ratings
    ratings.map(a => a.movieId).countByValue.toSeq.sortBy(-_._2).take(1)

//    which movie has received maximum 5 rating
    ratings.filter(a => a.rating == 5).map(rating => (rating.movieId, rating.rating)).countByValue.toSeq.sortBy(-_._2)

//  users rated genres
      ratings.map(a => (a.userId, a.movieId))




//user rated genres
//    users.join(movies)
  }

}
