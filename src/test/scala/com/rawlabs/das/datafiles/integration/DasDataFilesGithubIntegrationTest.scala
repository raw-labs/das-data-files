/*
 * Copyright 2025 RAW Labs S.A.
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

import com.rawlabs.das.datafiles.multiformat.DASDataFiles
import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSdkPermissionDeniedException, DASSettings}

class DasDataFilesGithubIntegrationTest extends AnyFlatSpec with Matchers {

  private val gitHubToken = sys.env.getOrElse("TEST_GITHUB_API_TOKEN", "")

  implicit private val settings: DASSettings = new DASSettings()
  behavior of "das data files with github"

  it should "create a table from a public GitHub repo file (no token)" in {
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
      val das = new DASDataFiles(config)
      val table = das.tables.head._2
      table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1)).hasNext
    }
    e.getMessage should be("Repository raw-labs/raw does not exist or requires credentials")
  }

  it should "fail if GitHub token is invalid for a private repo" in {
    val config =
      Map(
        "github_api_token" -> "INVALID_TOKEN",
        "paths" -> "1",
        "path0_url" -> "github://raw-labs/raw/master/docs/public/static/rql2DocsGenerated.json",
        "path0_format" -> "json")

    val e = intercept[DASSdkPermissionDeniedException] {
      val das = new DASDataFiles(config)
      val table = das.tables.head._2
      table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1)).hasNext
    }
    e.getMessage should be("Permission denied: github://raw-labs/raw/master/docs/public/static/rql2DocsGenerated.json")
  }

  it should "fail if the GitHub file does not exist" in {
    assume(gitHubToken.nonEmpty, "A GitHub API token is required for this test.")
    val config = Map(
      "github_api_token" -> gitHubToken,
      "paths" -> "1",
      "path0_url" -> "github://raw-labs/raw/master/does-not-exist.csv",
      "path0_format" -> "csv")

    val e = intercept[DASSdkInvalidArgumentException] {
      val das = new DASDataFiles(config)
      val table = das.tables.head._2
      table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1)).hasNext
    }
    e.getMessage should be("File not found: github://raw-labs/raw/master/does-not-exist.csv")
  }

  it should "fail if the GitHub path is actually a directory with no direct file" in {
    assume(gitHubToken.nonEmpty, "A GitHub API token is required for this test.")
    val config = Map(
      "github_api_token" -> gitHubToken,
      "paths" -> "1",
      "path0_url" -> "github://raw-labs/raw/master/docs/public/static/",
      "path0_format" -> "csv")

    val e = intercept[DASSdkInvalidArgumentException] {
      val das = new DASDataFiles(config)
      val definitions = das.tableDefinitions
      println(definitions)
      val table = das.tables.head._2
      table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1)).hasNext
    }
    e.getMessage should be("url refers to a directory: github://raw-labs/raw/master/docs/public/static/")
  }

}
