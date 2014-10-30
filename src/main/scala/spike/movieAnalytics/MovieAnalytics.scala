package spike.movieAnalytics

trait MovieGenre
case object Action extends MovieGenre
case object Adventure extends MovieGenre
case object Animation extends MovieGenre
case object Children extends MovieGenre
case object Comedy extends MovieGenre
case object Crime extends MovieGenre
case object Documentary extends MovieGenre
case object Drama extends MovieGenre
case object Fantasy extends MovieGenre
case object FilmNoir extends MovieGenre
case object Horror extends MovieGenre
case object Musical extends MovieGenre
case object Mystery extends MovieGenre
case object Romance extends MovieGenre
case object SciFi extends MovieGenre
case object Thriller extends MovieGenre
case object War extends MovieGenre
case object Western extends MovieGenre
object MovieGenre {
  def apply(input: String): MovieGenre = input match {
    case "Action"       => Action
    case "Adventure"    => Adventure
    case "Animation"    => Animation
    case "Children's"   => Children
    case "Comedy"       => Comedy
    case "Crime"        => Crime
    case "Documentary"  => Documentary
    case "Drama"        => Drama
    case "Fantasy"      => Fantasy
    case "Film-Noir"    => FilmNoir
    case "Horror"       => Horror
    case "Musical"      => Musical
    case "Mystery"      => Mystery
    case "Romance"      => Romance
    case "Sci-Fi"       => SciFi
    case "Thriller"     => Thriller
    case "War"          => War
    case "Western"      => Western
  }
}

case class MovieInfo(movieID: String, title: String, year: Option[Int], genres: Set[MovieGenre])

object MovieInfo {
  def apply(input: String): MovieInfo = {
    val movieInfoParts = input.split("::")
    val movieID = movieInfoParts(0)
    val movieNameRegex = """^([\w\s]+)\s\((\d+)\)$""".r

    val (title, movieYear) = movieInfoParts(1) match {
      case movieNameRegex(name, year) => (name, Some(year.toInt))
      case movieNameRegex(name, null) => (name, None)
//      case _ => new IllegalArgumentException("invalid genre" + movieInfoParts(1))
    }

    val genres = movieInfoParts(2).split('|').map(MovieGenre.apply).toSet
    new MovieInfo(movieID, title, movieYear, genres)
  }
}

//MovieInfo("1::Toy Story (1995)::Animation|Children's|Comedy")