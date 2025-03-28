import SbtDASPlugin.autoImport.*

scalacOptions ++= Seq("-deprecation")

lazy val commonExclusions = Seq(
  ExclusionRule(organization = "org.slf4j"),
  ExclusionRule(organization = "com.fasterxml.jackson.databind"),
  ExclusionRule(organization = "com.fasterxml.jackson.core"))

lazy val root = (project in file("."))
  .enablePlugins(SbtDASPlugin)
  .settings(
    repoNameSetting := "das-data-files",
    libraryDependencies ++= Seq(
      // DAS
      "com.raw-labs" %% "das-server-scala" % "0.6.2" % "compile->compile;test->test" excludeAll (commonExclusions *),
      // spark hadoop dependencies
      "org.apache.spark" %% "spark-sql" % "3.5.5",
      "com.databricks" %% "spark-xml" % "0.18.0",
      "org.apache.hadoop" % "hadoop-aws" % "3.4.1",
      "org.apache.hadoop" % "hadoop-common" % "3.4.1",
      // for github filesystem
      "org.kohsuke" % "github-api" % "1.327" excludeAll (commonExclusions *),
      // ScalaTest for unit tests
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % "test"),
    // Special env vars for spark
    dockerEnvVars += "JDK_JAVA_OPTIONS" ->
      ("--add-opens=java.base/java.io=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"),
    dockerEnvVars += "HADOOP_HOME" -> "/usr/local/hadoop",
    // Override dependencies because of security vulnerabilities
    dependencyOverrides ++= Seq(
      "org.apache.hadoop" % "hadoop-client-runtime" % "3.4.1",
      "io.netty" % "netty-handler" % "4.1.118.Final",
      "org.apache.ivy" % "ivy" % "2.5.2"))

Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")

run / javaOptions ++= Seq(
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
