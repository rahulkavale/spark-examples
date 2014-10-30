import spike.GithubData
case class GitHubActor(blog: Option[String], company: Option[String], email: Option[String], name: Option[String], actorType: Option[String])
object GitHubActor1{
  def parse(row: org.apache.spark.sql.Row): GitHubActor = {
    GitHubActor(row(0).asInstanceOf[Option[String]],
      row(1).asInstanceOf[Option[String]],
      row(2).asInstanceOf[Option[String]],
      row(3).asInstanceOf[Option[String]],
      row(4).asInstanceOf[Option[String]])
  }
}

case class A(a: Int)
object A{
  def apply(a1: String, b1: String): A = {
    A((a1 + b1).toInt)
  }
}

//case class GitHubActor(blog: String, company: String, email: String, name: String, actorType: String)
//object GitHubActor1{
//  def parse(row: org.apache.spark.sql.Row): GitHubActor = {
//    GitHubActor(row(0).asInstanceOf[String],
//      row(1).asInstanceOf[String],
//      row(2).asInstanceOf[String],
//      row(3).asInstanceOf[String],
//      row(4).asInstanceOf[String])
//  }
//}
