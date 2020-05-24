val projectVersion = "0.1.0-SNAPSHOT"
val projectScalaVersion = "2.12.10"
val kafkaVersion = "2.5.0"
val kafkaConnectClientVersion = "3.1.0"
val slf4jVersion = "1.7.30"

val repos = Seq(
  "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
  Resolver.mavenLocal
)

val dependencies = Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-tools" % kafkaVersion,
  "org.sourcelab" % "kafka-connect-client" % kafkaConnectClientVersion,
  "org.slf4j" % "slf4j-log4j12" % slf4jVersion
)

sourceGenerators in Compile += Def.task {
  val file = (sourceManaged in Compile).value / "com" / "github" / "mmolimar" / "kukulcan" / "BuildInfo.scala"
  IO.write(
    file,
    s"""package com.github.mmolimar.kukulcan
       |private[kukulcan] object BuildInfo {
       |  val Version = "${version.value}"
       |}""".stripMargin
  )
  Seq(file)
}.taskValue

val common = Seq(
  organization := "com.github.mmolimar",
  name := "kukulcan",
  version := projectVersion,
  scalaVersion := projectScalaVersion,
  crossScalaVersions := Seq("2.11.12", projectScalaVersion),
  resolvers ++= repos,
  libraryDependencies ++= dependencies
)

lazy val root = project.in(file("."))
  .settings(common)
