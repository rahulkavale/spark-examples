package spike.movieAnalytics

trait Gender
object Gender {
  case object M extends Gender
  case object F extends Gender

  def apply(input: String) = input match {
    case "M" => M
    case "F" => F
  }
}
case class ZipCode(code: String)

case class User(userID: Int,gender: Gender, age: String, occupation: String, zipCode: ZipCode)
object User {

  val professions = Map(
  0    -> "other or not specified",
  1    ->  "academic/educator",
  2    ->  "artist",
  3    ->  "clerical/admin",
  4    ->  "college/grad student",
  5    ->  "customer service",
  6    ->  "doctor/health care",
  7    ->  "executive/managerial",
  8    ->  "farmer",
  9    ->  "homemaker",
  10   ->  "K-12 student",
  11   ->  "lawyer",
  12   ->  "programmer",
  13   ->  "retired",
  14   ->  "sales/marketing",
  15   ->  "scientist",
  16   ->  "self-employed",
  17   ->  "technician/engineer",
  18   ->  "tradesman/craftsman",
  19   ->  "unemployed",
  20   ->  "writer"
  )

  val ageMappings = Map(
   1 ->  "Under 18",
  18 ->  "18-24",
  25 ->  "25-34",
  35 ->  "35-44",
  45 ->  "45-49",
  50 ->  "50-55",
  56 ->  "56+"
  )
  def apply(input: String): User = {
    //UserID::Gender::Age::Occupation::Zip-code
    val userInfoParts = input.split("::")
    val userId = userInfoParts(0).toInt
    val gender = Gender(userInfoParts(1))
    val age = ageMappings(userInfoParts(2).toInt)
    val occupation = professions(userInfoParts(3).toInt)
    val zipCode = ZipCode(userInfoParts(4))

    new User(userId, gender, age, occupation, zipCode)
  }
}