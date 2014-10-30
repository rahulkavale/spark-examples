package spike

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.{Elem, Node}

object StackOverflow {


  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Github data").setMaster("local"))

    val badges = loadBadges(sc)
    badges.take(1)
    val comments = loadComments(sc)
    val posts = loadPosts(sc)
    posts.take(1)
//    val tags = loadTags(sc)
    val users = loadUsers(sc)
    users.take(1)


  }

  case class Badge(id: Int, name: String, userId: Int)

  def loadBadges(sc: SparkContext): RDD[Badge] = {
    val badgesFile = sc.textFile("/Users/Rahul/Downloads/chemistry.stackexchange.com/Badges.xml")

    badgesFile.map {
      case a if a.contains("<?xml") | a.contains("<badges") => None
      case a =>
        val node = scala.xml.XML.loadString(a)
        val userId = node.attribute("UserId").get(0).text.toInt
        val name = node.attribute("Name").get(0).text
        val id = node.attribute("Id").get(0).text.toInt
        Some(Badge(id, name, userId))
    }.filter(a => a.isDefined).map(a => a.get)
  }

//  TODO handle creation date as Date and not string
  case class Comment(id: Int, postId: Int, score: Int, text: String, userId: String, creationDate: String)

    <row Id="1"
         PostId="3"
         Score="3"
         Text="Can you explain why &quot;this attraction is much stronger than the relatively weak repulsion between electrons&quot;? Why are the repulsions weak?"
         CreationDate="2012-04-25T18:30:51.367"
         UserId="18"
    />

  def loadComments(sc: SparkContext): RDD[Comment] = {
    val commentsFile = sc.textFile("/Users/Rahul/Downloads/chemistry.stackexchange.com/comments.xml")

    commentsFile.map {
      case a if a.contains("<?xml") | a.contains("<comments") => None
      case a =>
        val node = scala.xml.XML.loadString(a)
        val id = node.attribute("Id").get(0).text.toInt
        val postId = handleEmptyIntNode(node.attribute("PostId"))
        val score = handleEmptyIntNode(node.attribute("Score"))
        val text = handleEmptyStringNode(node.attribute("Text"))
        val userId = node.attribute("UserId").get(0).text
        val creationDate = node.attribute("CreationDate").get(0).text
        Some(Comment(id, postId, score, text, userId, creationDate))
    }.filter(a => a.isDefined).map(a => a.get)

  }
  case class Post(id: Int, postTypeId: Int, acceptedAnswerId: Int, score: Int, viewCount: Int, body: String, tags: String, answerCount: Int, commentCount: Int, favCount: Int, creationDate: String)

 val postsFilePath = "/Users/Rahul/spark-1.1.0/posts1.xml"

    <row Id="1"
         PostTypeId="1"
         AcceptedAnswerId="3"
         CreationDate="2012-04-25T18:24:34.340"
         Score="19"
         ViewCount="3510"
         Body="&lt;p&gt;It seems to me that the addition of electrons and protons as you move across a period would cause an atom to become larger. However, I'm told it gets smaller. Why is this?&lt;/p&gt;&#xA;"
         OwnerUserId="16"
         LastActivityDate="2014-05-08T18:00:40.150"
         Title="Why do atoms generally become smaller as one moves left to right across a period?"
         Tags="&lt;electrons&gt;&lt;atoms&gt;&lt;periodic-trends&gt;"
         AnswerCount="3"
         CommentCount="6"
         FavoriteCount="1" />
  def loadPosts(sc: SparkContext, postsFile: String = postsFilePath): RDD[Post] = {
    val post = sc.textFile(postsFile)

    post.map {
      case a if a.contains("<?xml") | a.contains("<(/){0,}posts") => None
      case a =>
        parsePost(a)
    }.filter(a => a.isDefined).map(a => a.get)

  }

  def parsePost(input: String): Option[Post] = {
    val node = scala.xml.XML.loadString(input)
    createPost(node)
  }

  def createPost(node: Elem): Some[Post] = {
    val id = node.attribute("Id").get(0).text.toInt
    println("id ============================", id)
    val postTypeId = node.attribute("PostTypeId").get(0).text.toInt
    val acceptedAnswerId = handleEmptyIntNode(node.attribute("AcceptedAnswerId"))
    val creationDate = node.attribute("CreationDate").get(0).text
    val score = handleEmptyIntNode(node.attribute("Score"))
    val body = handleEmptyStringNode(node.attribute("Body"))
    //        val ownerUserId = node.attribute("OwnerUserId").get(0).text.toInt
    val viewCount = handleEmptyIntNode(node.attribute("ViewCount"))
    //        val title = node.attribute("Title").get(0).text
    val tags = handleEmptyStringNode(node.attribute("Tags"))
    val answerCount = handleEmptyIntNode(node.attribute("AnswerCount"))
    val commentCount = handleEmptyIntNode(node.attribute("CommentCount"))
    val favCount = handleEmptyIntNode(node.attribute("FavoriteCount"))

    Some(Post(id, postTypeId, acceptedAnswerId, score, viewCount, body, tags, answerCount, commentCount, favCount, creationDate))
  }
  def handleEmptyIntNode(node: Option[Seq[Node]], default: Int = 0): Int = {
    if(node.isEmpty) default
    else if(node.get.isEmpty) default
    else node.get(0).text.toInt
  }

  def handleEmptyStringNode(node: Option[Seq[Node]], default: String = ""): String = {
    if(node.isEmpty) default
    else if(node.get.isEmpty) default
    else node.get(0).text
  }



  case class User(id: Int, reputation: Int, creationDate: String, displayName: String, lastAccessDate: String, websiteUrl: String, location: String, aboutMe: String, views: Int, upVotes: Int, downVotes: Int, accountId: Int)

  def loadUsers(sc: SparkContext): RDD[User] = {
    val users = sc.textFile("/Users/Rahul/Downloads/chemistry.stackexchange.com/users.xml")

    users.map {
      case a if a.contains("<?xml") | a.contains("<users") => None
      case a =>
        val node = scala.xml.XML.loadString(a)
        val id = node.attribute("Id").get(0).text.toInt
        val reputation = node.attribute("Reputation").get(0).text.toInt
        val creationDate = node.attribute("CreationDate").get(0).text
        val displayName = node.attribute("DisplayName").get(0).text
        val lastAccessDate = node.attribute("LastAccessDate").get(0).text
        val websiteUrl = node.attribute("WebsiteUrl").get(0).text
        val location = node.attribute("Location").get(0).text
        val aboutMe = node.attribute("AboutMe").get(0).text
        val views = node.attribute("Views").get(0).text.toInt
        val upVotes = node.attribute("UpVotes").get(0).text.toInt
        val downVotes = node.attribute("DownVotes").get(0).text.toInt
        val accountId = node.attribute("AccountId").get(0).text.toInt
        Some(User(id, reputation, creationDate, displayName, lastAccessDate, websiteUrl, location, aboutMe, views, upVotes, downVotes, accountId))
    }.filter(a => a.isDefined).map(a => a.get)
  }

    <row Id="-1"
         Reputation="1"
         CreationDate="2012-04-25T15:50:17.463"
         DisplayName="Community"
         LastAccessDate="2012-04-25T15:50:17.463"
         WebsiteUrl="http://meta.stackexchange.com/"
         Location="on the server farm"
         AboutMe="&lt;p&gt;Hi, I'm not really a person.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I'm a background process that helps keep this site clean!&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I do things like&lt;/p&gt;&#xA;&#xA;&lt;ul&gt;&#xA;&lt;li&gt;Randomly poke old unanswered questions every hour so they get some attention&lt;/li&gt;&#xA;&lt;li&gt;Own community questions and answers so nobody gets unnecessary reputation from them&lt;/li&gt;&#xA;&lt;li&gt;Own downvotes on spam/evil posts that get permanently deleted&lt;/li&gt;&#xA;&lt;li&gt;Own suggested edits from anonymous users&lt;/li&gt;&#xA;&lt;li&gt;&lt;a href=&quot;http://meta.stackexchange.com/a/92006&quot;&gt;Remove abandoned questions&lt;/a&gt;&lt;/li&gt;&#xA;&lt;/ul&gt;&#xA;"
         Views="0"
         UpVotes="840"
         DownVotes="670"
         AccountId="-1" />


}

