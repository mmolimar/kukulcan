val projectVersion = "0.2.0-SNAPSHOT"

val repos = Seq(
  "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
  "jitpack" at "https://jitpack.io",
  Resolver.mavenLocal
)

lazy val settings = new {
  val projectScalaVersion = "2.12.13"

  val dependencies = new {
    val asciiGraphsVersion = "0.0.6"
    val circeVersion = "0.13.0"
    val confluentVersion = "6.1.0"
    val kafkaVersion = "2.7.0"
    val kafkaConnectClientVersion = "3.1.0"

    val scalaTestVersion = "3.2.3"

    val exclusions = ExclusionRule(organization = "org.apache.kafka")
    val api = Seq(
      "org.scala-lang" % "scala-compiler" % projectScalaVersion,
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "org.apache.kafka" % "kafka-tools" % kafkaVersion,
      "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
      "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion,
      "org.sourcelab" % "kafka-connect-client" % kafkaConnectClientVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "io.confluent" % "kafka-schema-registry-client" % confluentVersion excludeAll exclusions,
      "io.confluent" % "kafka-json-schema-provider" % confluentVersion excludeAll exclusions,
      "io.confluent" % "kafka-protobuf-provider" % confluentVersion excludeAll exclusions,
      "io.confluent.ksql" % "ksqldb-cli" % confluentVersion excludeAll exclusions,
      "com.github.mutcianm" %% "ascii-graphs" % asciiGraphsVersion,

      "org.scalatest" %% "scalatest-wordspec" % scalaTestVersion % Test,
      "org.scalatest" %% "scalatest-shouldmatchers" % scalaTestVersion % Test,
      "org.apache.kafka" % "connect-runtime" % kafkaVersion % Test,
      "org.apache.kafka" % "connect-file" % kafkaVersion % Test,
      "io.github.embeddedkafka" %% "embedded-kafka-schema-registry" % confluentVersion % Test excludeAll exclusions,
      "io.confluent.ksql" % "ksqldb-rest-app" % confluentVersion % Test excludeAll exclusions
    )
    val repl = Seq.empty
    val root = Seq.empty
  }
  val common = Seq(
    organization := "com.github.mmolimar",
    version := projectVersion,
    scalaVersion := projectScalaVersion,
    resolvers ++= repos,
    licenses := Seq("Apache License, Version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(Developer(
      "mmolimar",
      "Mario Molina",
      "",
      url("https://github.com/mmolimar")
    )),
    compileOrder := CompileOrder.ScalaThenJava,
    javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint:unchecked"),
    scalacOptions ++= Seq("-deprecation", "-feature")
  )
  val root = Seq(
    name := "kukulcan",
    javacOptions ++= Seq(
      "--add-exports=jdk.jshell/jdk.internal.jshell.tool=ALL-UNNAMED"
    ),
    packExpandedClasspath := true,
    packGenerateMakefile := false,
    publish / skip := true,
    libraryDependencies ++= dependencies.root
  )
  val api = Seq(
    name := "kukulcan-api",
    libraryDependencies ++= dependencies.api,
    parallelExecution in Test := false
  )
  val repl = Seq(
    name := "kukulcan-repl",
    sourceGenerators in Compile += {
      Def.task {
        val file = (sourceManaged in Compile).value / "com" / "github" / "mmolimar" / "kukulcan" / "repl" / "BuildInfo.scala"
        IO.write(
          file,
          s"""package com.github.mmolimar.kukulcan.repl
             |private[repl] object BuildInfo {
             |  val version = "${version.value}"
             |}""".stripMargin
        )
        Seq(file)
      }.taskValue
    },
    javacOptions ++= Seq(
      "--add-exports=jdk.jshell/jdk.internal.jshell.tool=ALL-UNNAMED"
    ),
    libraryDependencies ++= dependencies.repl
  )
  val pykukulcan = Seq(
    name := "pykukulcan"
  )
}

lazy val apiProject = project
  .in(file("kukulcan-api"))
  .settings(
    settings.common,
    settings.api
  )
lazy val replProject = project
  .in(file("kukulcan-repl"))
  .dependsOn(apiProject)
  .settings(
    settings.common,
    settings.repl
  )
lazy val pykukulcanProject = project
  .in(file("python"))
  .settings(
    settings.pykukulcan
  )
lazy val root = project
  .in(file("."))
  .dependsOn(apiProject, replProject)
  .enablePlugins(PackPlugin)
  .enablePlugins(KukulcanPackPlugin)
  .settings(
    settings.common,
    settings.root
  )
  .aggregate(apiProject, replProject)
