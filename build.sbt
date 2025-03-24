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
      "org.apache.hadoop" % "hadoop-aws" % "3.4.1" exclude ("io.netty", "netty-handler"),
      "org.apache.hadoop" % "hadoop-common" % "3.4.1",
      // for github filesystem
      "org.kohsuke" % "github-api" % "1.327" excludeAll (
        ExclusionRule(organization = "org.slf4j"),
        ExclusionRule(organization = "com.fasterxml.jackson.databind"),
        ExclusionRule(organization = "com.fasterxml.jackson.core")),
      // ScalaTest for unit tests
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % "test"),
    dependencyOverrides ++= Seq(
      // Overrides because of security vulnerabilities
      // Jackson (multiple CVEs: CVE-2022-42003, CVE-2022-42004, etc.)
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.2",
      // hadoop-client-runtime-3.3.4 was pulling in a lot of old dependencies (hadoop-client-runtime is pulled by spark)
      "org.apache.hadoop" % "hadoop-client-runtime" % "3.4.1" exclude ("org.apache.avro", "avro"),

      // Protobuf (CVE-2021-22569, CVE-2022-3509, CVE-2024-7254, etc.)
      "com.google.protobuf" % "protobuf-java" % "3.25.5",

      // Nimbus JOSE+JWT (CVE-2023-52428)
      "com.nimbusds" % "nimbus-jose-jwt" % "9.37.2",

      // Commons IO (CVE-2024-47554)
      "commons-io" % "commons-io" % "2.18.0",

      // dnsjava (CVE-2024-25638)
      "dnsjava" % "dnsjava" % "3.6.3",

      // Netty (CVE-2025-24970)
      "io.netty" % "netty-handler" % "4.1.118.Final",

      // json-smart (CVE-2021-31684, CVE-2023-1370)
      "net.minidev" % "json-smart" % "2.5.2",

      // Apache Avro (CVE-2024-47561, CVE-2023-39410)
      "org.apache.avro" % "avro" % "1.12.0",

      // Apache Ivy (CVE-2022-46751)
      "org.apache.ivy" % "ivy" % "2.5.3"))
