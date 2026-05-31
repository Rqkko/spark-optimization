
name := "spark-optimization"

version := "0.3"

scalaVersion := "2.13.16"

val sparkVersion = "4.0.0"
val log4jVersion = "2.24.3"

javacOptions ++= Seq("-source", "21", "-target", "21")

resolvers ++= Seq(
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
)