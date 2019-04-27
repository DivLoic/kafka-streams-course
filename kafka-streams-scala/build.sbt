name          := "kafka-streams-scala"
organization  := "com.github.simplesteph.udemy.kafka.streams"
version       := "2.0-SNAPSHOT"
scalaVersion  := "2.12.8"

lazy val `kafka-streams-course` = (project in file(".")).aggregate(
  `course-intro`,
  `word-count`,
  `favourite-player`,
  `arcade-contest-exactly-once`,
  `user-event-enricher`,
  `streaming-app-testing`,
  `avro-messages-scala`,
  `windowed-leaderboard-scala`,
  `suppress-operator`,
  `processor-api-scala`
)

lazy val `course-intro` = project in file("0-course-intro")

lazy val `word-count` = project in file("1-word-count")

lazy val `favourite-player` = project in file("2-favourite-player-scala")

lazy val `arcade-contest-exactly-once` = project in file("3-arcade-contest-exactly-once-scala")

lazy val `user-event-enricher` = project in file("4-user-event-enricher-scala")

lazy val `streaming-app-testing` = project in file("5-streaming-app-testing-scala")

lazy val `avro-messages-scala` = project in file("6-avro-messages-scala")

lazy val `windowed-leaderboard-scala` = project in file("7-windowed-leaderboard-scala")

lazy val `suppress-operator` = project in file("8-suppress-operator-scala")

lazy val `processor-api-scala` = project in file("9-processor-api-scala")
