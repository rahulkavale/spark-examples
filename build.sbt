name := "SparkPoc"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.1.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Spray Repository" at "http://repo.spray.cc/"
