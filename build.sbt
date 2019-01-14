name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.18",
  "com.typesafe.akka" %% "akka-actor" % "2.5.18",
  "com.typesafe.akka" %% "akka-actor-typed" % "2.5.18",
  "com.typesafe.akka" %% "akka-slf4j" % "2.5.18",
  
  "com.typesafe.akka" %% "akka-http" % "10.1.5",

  "javax.jms" % "jms" % "1.1",
  "org.apache.activemq" % "activemq-client" % "5.15.7",
  "org.apache.activemq" % "activemq-broker" % "5.15.7",
  "com.lightbend.akka" %% "akka-stream-alpakka-jms" % "1.0-M1",
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "1.0-M1",
  "mysql" % "mysql-connector-java" % "8.0.11",

  "com.typesafe.akka" %% "akka-stream-kafka" % "1.0-M1",
  "org.apache.kafka" %% "kafka" % "2.0.0",
  "org.apache.kafka" % "kafka-streams" % "2.0.0",

  "com.lightbend.akka" %% "akka-stream-alpakka-sse" % "1.0-M1",
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "1.0-M1",
  "com.lightbend.akka" %% "akka-stream-alpakka-xml" % "1.0-M1",
  
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.18"  % "test",
  "com.typesafe.play" %% "play" % "2.6.10",
  "com.geteventstore" %% "eventstore-client" % "4.1.1",
  "com.github.andyglow" %% "websocket-scala-client" % "0.2.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.10",
  "org.apache.httpcomponents" % "httpclient" % "4.5.6",
  "commons-io" % "commons-io" % "2.5",
  "org.apache.avro" % "avro" % "1.8.2",
  "com.twitter" %% "bijection-avro" % "0.9.6"
)

resolvers ++= Seq(
  "repository.jboss.org-public" at "https://repository.jboss.org/nexus/content/groups/public",
  "Mvnrepository" at "https://mvnrepository.com/artifact"
)

//see: https://github.com/sbt/sbt/issues/3618
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}

fork in run := true