name := "kafka-streams-course"
organization := "com.github.simplesteph.udemy.kafka.streams"
version := "2.0-SNAPSHOT"
scalaVersion := "2.12.8"

lazy val `kafka-streams-course` = (project in file(".")).aggregate(
  `datagen-stream-fighter`,
  `favourite-player`,
  `arcade-contest-exactly-once`,
  `user-event-enricher`,
  `streaming-app-testing`,
  `user-avro-event-enricher`
)

lazy val `datagen-stream-fighter` = project in file("0-datagen-stream-fighter")

lazy val `favourite-player` =
  (project in file("2-favourite-player-scala"))
    .dependsOn(`datagen-stream-fighter`)

lazy val `arcade-contest-exactly-once` =
  (project in file("3-arcade-contest-exactly-once-scala"))
    .dependsOn(`datagen-stream-fighter`)

lazy val `user-event-enricher` =
  (project in file("4-user-event-enricher-scala"))
    .dependsOn(`datagen-stream-fighter`)

lazy val `streaming-app-testing` =
  (project in file("5-streaming-app-testing-scala"))
    .dependsOn(`datagen-stream-fighter`)

lazy val `user-avro-event-enricher` =
  (project in file("6-user-avro-event-enricher-scala"))
    .dependsOn(`datagen-stream-fighter`)

