import sbt.Keys.libraryDependencies

course := "reactive"
assignment := "actorbintree"

Test / parallelExecution := false

val akkaVersion = "2.6.19"

scalaVersion := "3.1.0"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
)
//Compile / run / mainClass := Some("lab3.MessageBroker")
Compile / run / mainClass := Some("lab3.Consumer")
//Compile / run / mainClass := Some("lab3.Producer")