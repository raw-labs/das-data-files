import SbtDASPlugin.autoImport.*

lazy val root = (project in file("."))
  .enablePlugins(SbtDASPlugin)
  .settings(
    repoNameSetting := "das-data-files",
    libraryDependencies ++= Seq(
      // DAS
      "com.raw-labs" %% "das-server-scala" % "0.6.0" % "compile->compile;test->test" excludeAll (
        ExclusionRule(organization = "org.slf4j"),
        ExclusionRule(organization = "com.fasterxml.jackson.databind"),
        ExclusionRule(organization = "com.fasterxml.jackson.core")),
      // spark hadoop dependencies
      "org.apache.spark" %% "spark-sql" % "3.5.5",
      "com.databricks" %% "spark-xml" % "0.18.0",
      "org.apache.hadoop" % "hadoop-aws" % "3.4.1",
      "org.apache.hadoop" % "hadoop-common" % "3.4.1",
      // for github filesystem
      "org.kohsuke" % "github-api" % "1.327" excludeAll (
        ExclusionRule(organization = "org.slf4j"),
        ExclusionRule(organization = "com.fasterxml.jackson.databind"),
        ExclusionRule(organization = "com.fasterxml.jackson.core")),
      // ScalaTest for unit tests
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % "test"),
    dependencyOverrides ++= Seq("io.netty" % "netty-handler" % "4.1.118.Final"))
