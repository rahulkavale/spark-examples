import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

seq(assemblySettings: _*)

name := "SparkPoc"

version := "1.0"

scalaVersion := "2.10.3"

retrieveManaged in ThisBuild := true

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.1.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.0"

libraryDependencies += "joda-time" % "joda-time" % "2.0"

libraryDependencies += "org.joda" % "joda-convert" % "1.2"


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Spray Repository" at "http://repo.spray.cc/"

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  val excludes = Set(
    "jsp-api-2.1-6.1.14.jar",
    "jsp-2.1-6.1.14.jar",
    "jasper-compiler-5.5.12.jar",
    "commons-beanutils-core-1.8.0.jar",
    "commons-beanutils-1.7.0.jar",
    "servlet-api-2.5-20081211.jar",
    "servlet-api-2.5.jar",
    "jcl-over-slf4j-1.7.5.jar"
  )
  cp filter { jar => excludes(jar.data.getName) }
}

mergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case x if x.startsWith("META-INF") => MergeStrategy.discard // Bumf
    case x if x.startsWith("plugin.properties") => MergeStrategy.discard // Bumf
    case x if x.endsWith(".html") => MergeStrategy.discard // More bumf
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last // For Log$Logger.class
    case x => old(x)
  }
}