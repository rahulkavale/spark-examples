package spike

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

case class GitHubActor(blog: String, company: String, email: String, name: String, actorType: String)
object GitHubActor{
  def parse(row: org.apache.spark.sql.Row): GitHubActor = {
    GitHubActor(row(0).asInstanceOf[String],
      row(1).asInstanceOf[String],
      row(2).asInstanceOf[String],
      row(3).asInstanceOf[String],
      row(4).asInstanceOf[String])
  }
}

object GithubData {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Github data").setMaster("local"))

    val sqlContext = new SQLContext(sc)
    val githubJson = sqlContext.jsonFile("/tmp/2014-09-23-1.json.gz")
    githubJson.registerTempTable("githubData")
    val actors = sqlContext.sql("select actor_attributes.blog, actor_attributes.company, actor_attributes.email, actor_attributes.gravatar_id, actor_attributes.location, actor_attributes.login, actor_attributes.name, actor_attributes.type from githubData")
    val a1 = actors.first()
    val githubActors = actors.map(GitHubActor.parse)
    val actorBlogs = githubActors.map(actor => (actor.name, actor.blog))
    actorBlogs.saveAsTextFile("/tmp/actor-blogs")
  }
  
  def exploringSqlAPI(sc: SparkContext) = {
    val inputFile = "/Users/Rahul/projects/spark/dataset/2014-09-23-0.json"
    val outputFile = "/Users/Rahul/projects/spark/dataset/2014-09-23-0-name-handle-mappings.json"

    val sqlContext = new SQLContext(sc)
    val githubRepositories = sqlContext.jsonFile(inputFile)
    githubRepositories.registerTempTable("repositories")

    val organizations = sqlContext.sql("Select repository.organization from repositories")
    val nonNullOrganizationNames = organizations.filter(a => !a.isNullAt(0))

    val nameHandleMappings = sqlContext.sql("Select payload.actor, actor_attributes.actor_attributes from r")
    val q = nameHandleMappings.take(1)(0)
    val nameHandleMappings1 = sqlContext.sql("Select actor_attributes.actor_attributes from repositories")
    nameHandleMappings.saveAsTextFile(outputFile)
  }
}
