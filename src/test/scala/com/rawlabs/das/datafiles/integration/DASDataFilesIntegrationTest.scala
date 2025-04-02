/*
 * Copyright 2024 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.datafiles.integration

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.datafiles.SparkTestContext
import com.rawlabs.das.datafiles.multiformat.DASDataFiles
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSdkPermissionDeniedException, DASSettings}

class DASDataFilesIntegrationTest extends AnyFlatSpec with Matchers with SparkTestContext {

  private val awsAccessKey = sys.env.getOrElse("RAW_AWS_ACCESS_KEY_ID", "")
  private val awsSecretKey = sys.env.getOrElse("RAW_AWS_SECRET_ACCESS_KEY", "")
  private val gitHubToken = sys.env.getOrElse("TEST_GITHUB_API_TOKEN", "")

  implicit private val settings: DASSettings = new DASSettings()

  behavior of "das data files with s3 files"

  it should "create a table from a public s3 bucket without credentials" in {
    val config = Map(
      "aws_region" -> "eu-west-1",
      "paths" -> "1",
      "path0_url" -> "s3://rawlabs-public-test-data/winter_olympics.csv",
      "path0_format" -> "csv",
      "path0_name" -> "winter_olympics")

    val das = new DASDataFiles(config)
    das.tables.size shouldBe 1
    das.tables.head._1 shouldBe "winter_olympics"

    val table = das.tables.head._2
    val it = table.execute(quals = Seq.empty, columnsRequested = Seq.empty, sortKeys = Seq.empty, maybeLimit = Some(1))
    it.hasNext shouldBe true

    it.next().getColumns(0).getName should be("Year")
  }

  it should "create a table from a private s3 bucket with valid credentials" in {
    // This test requires that the user running the test has a private
    // bucket accessible with the AWS credentials provided.
    assume(awsAccessKey.nonEmpty && awsSecretKey.nonEmpty, "AWS credentials must be set for this test to run.")

    val config = Map(
      "aws_region" -> "eu-west-1",
      "aws_access_key" -> awsAccessKey,
      "aws_secret_key" -> awsSecretKey,
      "paths" -> "1",
      "path0_url" -> "s3://rawlabs-private-test-data/winter_olympics.csv",
      "path0_format" -> "csv",
      "path0_name" -> "winter_olympics")

    val das = new DASDataFiles(config)
    das.tables.size shouldBe 1
    das.tables.head._1 shouldBe "winter_olympics"

    val table = das.tables.head._2
    val it = table.execute(quals = Seq.empty, columnsRequested = Seq.empty, sortKeys = Seq.empty, maybeLimit = Some(1))
    it.hasNext shouldBe true
    it.next().getColumns(0).getName should be("Year")
  }

  it should "create multiple tables from an s3 path using wildcard *" in {
    // We should have 2 tables: summer_olympics and winter_olympics
    assume(awsAccessKey.nonEmpty && awsSecretKey.nonEmpty)

    val config = Map(
      "aws_region" -> "eu-west-1",
      "aws_access_key" -> awsAccessKey,
      "aws_secret_key" -> awsSecretKey,
      "paths" -> "1",
      "path0_url" -> "s3://rawlabs-private-test-data/*_olympics.csv",
      "path0_format" -> "csv"
      // no explicit path0_name => names will be derived from each matching file
    )

    val das = new DASDataFiles(config)
    das.tables.size should be(2)

    Seq("summer_olympics", "winter_olympics").foreach { tableName =>
      val table = das.tables(tableName)
      val it = table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
      it.hasNext shouldBe true
    }
  }

  it should "fail if credentials are invalid for a private bucket" in {
    val config = Map(
      "aws_region" -> "eu-west-1",
      "aws_access_key" -> "INVALID_ACCESS",
      "aws_secret_key" -> "INVALID_SECRET",
      "paths" -> "1",
      "path0_url" -> "s3://rawlabs-private-test-data/winter_olympics.csv",
      "path0_format" -> "csv")

    // We expect some sort of authentication/permission error:
    intercept[DASSdkPermissionDeniedException] {
      new DASDataFiles(config).tables.head._2
        .execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
        .hasNext
    }
  }

  it should "fail if missing credentials for a private bucket" in {
    val config =
      Map(
        "aws_region" -> "eu-west-1",
        "paths" -> "1",
        "path0_url" -> "s3://rawlabs-private-test-data/winter_olympics.csv",
        "path0_format" -> "csv")

    // We expect some sort of authentication/permission error:
    intercept[DASSdkPermissionDeniedException] {
      new DASDataFiles(config).tables.head._2
        .execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
        .hasNext
    }
  }

  it should "fail if missing credentials for a private bucket with wildcard" in {
    val config =
      Map(
        "aws_region" -> "eu-west-1",
        "paths" -> "1",
        "path0_url" -> "s3://rawlabs-private-test-data/*_olympics.csv",
        "path0_format" -> "csv")

    // We expect some sort of authentication/permission error:
    intercept[DASSdkPermissionDeniedException] {
      val das = new DASDataFiles(config)
      val table = das.tables.head._2
      table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1)).hasNext
    }
  }

  it should "fail if the file is missing on s3" in {
    val config = Map(
      "aws_region" -> "eu-west-1",
      "paths" -> "1",
      "path0_url" -> "s3://rawlabs-public-test-data/this_file_does_not_exist.csv",
      "path0_format" -> "csv")

    intercept[DASSdkInvalidArgumentException] {
      val das = new DASDataFiles(config)
      val table = das.tables.head._2
      table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1)).hasNext
    }
  }

  it should "fail if the s3 path is actually a directory rather than a file" in {
    // For instance, referencing a "prefix-only" path that does not contain actual data files
    // might cause an error or produce an empty result (depending on how the plugin is implemented).
    // If your plugin is coded to treat a directory as valid, adapt the test accordingly.
    val config =
      Map("paths" -> "1", "path0_url" -> "s3://rawlabs-public-test-data/demos", "path0_format" -> "csv")

    intercept[DASSdkInvalidArgumentException] {
      val das = new DASDataFiles(config)
      val table = das.tables.head._2
      table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1)).hasNext
    }
  }

  // -----------------------------------------------------------------------
  // Similar tests for GitHub
  // -----------------------------------------------------------------------

  behavior of "das data files with github"

  it should "create a table from a public GitHub repo file (no token)" in {
    // A public GitHub repo with a raw file
    // e.g., "github://owner/repo/main/path/to/file.csv"
    val config = Map(
      "paths" -> "1",
      "path0_url" -> "github://owid/covid-19-data/master/public/data/owid-covid-codebook.csv",
      "path0_format" -> "csv",
      "path0_name" -> "covid_codebook")

    val das = new DASDataFiles(config)
    das.tables.size shouldBe 1
    das.tables.head._1 shouldBe "covid_codebook"

    val table = das.tables.head._2
    val it = table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
    it.hasNext shouldBe true
    it.next().getColumns(0).getName should be("column")
  }

  it should "create a table from a private GitHub repo file (with token)" in {
    // This test requires a valid GitHub personal access token or GitHub App token
    // for a private repo that you control.
    assume(gitHubToken.nonEmpty, "A GitHub API token is required for this test.")

    val config = Map(
      "paths" -> "1",
      "github_api_token" -> gitHubToken,
      "path0_url" -> "github://raw-labs/raw/master/docs/public/static/rql2DocsGenerated.json",
      "path0_format" -> "json",
      "path0_name" -> "rql2_docs")

    val das = new DASDataFiles(config)
    das.tables.size shouldBe 1
    das.tables.head._1 shouldBe "rql2_docs"

    val table = das.tables.head._2
    val it = table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
    it.hasNext shouldBe true
  }

  it should "create tables from a public GitHub repo using wildcards" in {
    // Example: "github://owner/repo/main/data/*.csv"
    val config = Map(
      "paths" -> "1",
      "path0_url" -> "github://owid/covid-19-data/master/public/data/*.csv",
      "path0_format" -> "csv")

    val das = new DASDataFiles(config)
    das.tables.size should be >= 1
    // Similar to the S3 wildcard test, you can confirm multiple tables or
    // check the first one:
    val firstTableName = das.tables.head._1
    val firstTable = das.tables(firstTableName)
    val it = firstTable.execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
    it.hasNext shouldBe true
  }

  it should "fail if GitHub token is missing for a private repo" in {
    val config =
      Map(
        "paths" -> "1",
        "path0_url" -> "github://raw-labs/raw/master/docs/public/static/rql2DocsGenerated.json",
        "path0_format" -> "json")

    val e = intercept[DASSdkInvalidArgumentException] {
      // or DASSdkUnauthenticatedException / whichever your code throws
      new DASDataFiles(config).tables
    }
    assert(e.getMessage.contains("Repository raw-labs/raw does not exist or requires credentials"))
  }

  it should "fail if GitHub token is invalid for a private repo" in {
    val config =
      Map(
        "github_api_token" -> "INVALID_TOKEN",
        "paths" -> "1",
        "path0_url" -> "github://raw-labs/raw/master/docs/public/static/rql2DocsGenerated.json",
        "path0_format" -> "json")

    intercept[DASSdkPermissionDeniedException] {
      // or DASSdkUnauthenticatedException / whichever your code throws
      new DASDataFiles(config).tables
    }
  }

  it should "fail if the GitHub file does not exist" in {
    assume(gitHubToken.nonEmpty, "A GitHub API token is required for this test.")
    val config = Map(
      "github_api_token" -> gitHubToken,
      "paths" -> "1",
      "path0_url" -> "github://raw-labs/raw-labs/master/does-not-exist.csv",
      "path0_format" -> "csv")

    intercept[DASSdkInvalidArgumentException] {
      new DASDataFiles(config).tables
    }
  }

  it should "fail if the GitHub path is actually a directory with no direct file" in {
    assume(gitHubToken.nonEmpty, "A GitHub API token is required for this test.")
    val config = Map(
      "github_api_token" -> gitHubToken,
      "paths" -> "1",
      "path0_url" -> "github://raw-labs/raw-labs/master/docs/public/static/",
      "path0_format" -> "csv")

    intercept[DASSdkInvalidArgumentException] {
      new DASDataFiles(config).tables
    }
  }
}
