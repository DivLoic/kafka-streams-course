name := "8-suppress-operator-scala"
organization := "com.github.simplesteph.udemy.kafka.streams"
version := "2.0-SNAPSHOT"
scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % "2.3.0",
  "org.slf4j" % "slf4j-api" % "1.7.26",
  "org.slf4j" % "slf4j-log4j12" % "1.7.26",
  "io.circe" %% "circe-core" % "0.11.1",
  "io.circe" %% "circe-parser" % "0.11.1",
  "io.circe" %% "circe-generic" % "0.11.1",
)

// add avro and kafka dependencies
resolvers ++= Seq("confluent" at "http://packages.confluent.io/maven/")

libraryDependencies ++= Seq(
  "io.confluent" % "kafka-avro-serializer" % "5.3.0",
  "io.confluent" % "kafka-streams-avro-serde" % "5.3.0"
)

// data generation dependencies only
libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.14.0",
  "com.github.stevenchen3" %% "scala-faker" % "0.1.1"
)

// leverage java 8
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
scalacOptions := Seq("-target:jvm-1.8")
initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

avroSpecificSourceDirectories in Compile += (resourceDirectory in Compile).value / "avro"

sourceGenerators in Compile += (avroScalaGenerateSpecific in Compile).taskValue