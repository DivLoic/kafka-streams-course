name := "6-user-avro-event-enricher-scala"
organization        := "com.github.simplesteph.udemy.kafka.streams"
version             := "2.0-SNAPSHOT"
scalaVersion := "2.12.8"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % "2.2.0",
  "org.slf4j" %  "slf4j-api" % "1.7.26",
  "org.slf4j" %  "slf4j-log4j12" % "1.7.26",
  "io.circe" %% "circe-core" % "0.11.1",
  "io.circe" %% "circe-parser" % "0.11.1",
  "io.circe" %% "circe-generic" % "0.11.1"
)

// add avro and kafka dependencies
resolvers ++= Seq("confluent" at "http://packages.confluent.io/maven/")

libraryDependencies ++= Seq(
  "io.confluent" % "kafka-avro-serializer" % "5.3.0",
  "io.confluent" % "kafka-streams-avro-serde" % "5.3.0"
)

// leverage java 8
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
scalacOptions := Seq("-target:jvm-1.8")
initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

stringType in AvroConfig := "String"

sourceDirectory in AvroConfig := (resourceDirectory in Compile).value / "avro"