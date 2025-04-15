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

class DASDataFilesS3IntegrationTest extends AnyFlatSpec with Matchers {

  private val awsAccessKey = sys.env.getOrElse("RAW_AWS_ACCESS_KEY_ID", "")
  private val awsSecretKey = sys.env.getOrElse("RAW_AWS_SECRET_ACCESS_KEY", "")

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
    das.close()
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
    das.close()
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
    val e = intercept[DASSdkPermissionDeniedException] {
      val das = new DASDataFiles(config)
      val definitions = das.tableDefinitions
      println(definitions)
      val table = das.tables.head._2
      table
        .execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
        .hasNext
    }
    e.getMessage should be("Access denied: s3://rawlabs-private-test-data/winter_olympics.csv")
  }

  it should "fail if missing credentials for a private bucket" in {
    val config =
      Map(
        "aws_region" -> "eu-west-1",
        "paths" -> "1",
        "path0_url" -> "s3://rawlabs-private-test-data/winter_olympics.csv",
        "path0_format" -> "csv")

    val e = intercept[DASSdkPermissionDeniedException] {
      val das = new DASDataFiles(config)
      val definitions = das.tableDefinitions
      println(definitions)
      val table = das.tables.head._2
      table
        .execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
        .hasNext
    }
    e.getMessage should be("Access denied: s3://rawlabs-private-test-data/winter_olympics.csv")
  }

  it should "fail if missing credentials for a private bucket with wildcard" in {
    val config =
      Map(
        "aws_region" -> "eu-west-1",
        "paths" -> "1",
        "path0_url" -> "s3://rawlabs-private-test-data/*_olympics.csv",
        "path0_format" -> "csv")

    // We expect some sort of authentication/permission error:
    val e = intercept[DASSdkPermissionDeniedException] {
      val das = new DASDataFiles(config)
      val table = das.tables.head._2
      table.execute(Seq.empty, Seq.empty, Seq.empty, Some(1)).hasNext
    }
    e.getMessage should be(
      "Got forbidden while trying to resolve wildcard s3://rawlabs-private-test-data/*_olympics.csv")
  }

  it should "fail if the file is missing on s3" in {
    val config = Map(
      "aws_region" -> "eu-west-1",
      "paths" -> "1",
      "path0_url" -> "s3://rawlabs-public-test-data/this_file_does_not_exist.csv",
      "path0_format" -> "csv")

    val e = intercept[DASSdkInvalidArgumentException] {
      val das = new DASDataFiles(config)
      val definitions = das.tableDefinitions
      println(definitions)
      val table = das.tables.head._2
      table
        .execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
        .hasNext
    }
    e.getMessage should be("File not found: s3://rawlabs-public-test-data/this_file_does_not_exist.csv")
  }

  it should "fail if the s3 path is actually a directory rather than a file" in {
    val config =
      Map("paths" -> "1", "path0_url" -> "s3://rawlabs-public-test-data/demos", "path0_format" -> "csv")

    val e = intercept[DASSdkInvalidArgumentException] {
      val das = new DASDataFiles(config)
      val definitions = das.tableDefinitions
      println(definitions)
      val table = das.tables.head._2
      table
        .execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
        .hasNext
    }
    e.getMessage should be(
      "Could not infer s3://rawlabs-public-test-data/demos, please verify that the path is a valid csv file")
  }

  it should "fail if the s3 path is not a parquet file" in {
    val config =
      Map(
        "paths" -> "1",
        "path0_url" -> "s3://rawlabs-public-test-data/winter_olympics.csv",
        "path0_format" -> "parquet")

    val e = intercept[DASSdkInvalidArgumentException] {
      val das = new DASDataFiles(config)
      val definitions = das.tableDefinitions
      println(definitions)
      val table = das.tables.head._2
      table
        .execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
        .hasNext
    }
    e.getMessage should be(
      "Error while inferring s3://rawlabs-public-test-data/winter_olympics.csv, please verify that the path is a valid parquet file")
  }

  it should "fail if the s3 path is not a json file" in {
    val config =
      Map("paths" -> "1", "path0_url" -> "s3://rawlabs-public-test-data/winter_olympics.csv", "path0_format" -> "json")

    val e = intercept[DASSdkInvalidArgumentException] {
      val das = new DASDataFiles(config)
      val definitions = das.tableDefinitions
      println(definitions)
      val table = das.tables.head._2
      table
        .execute(Seq.empty, Seq.empty, Seq.empty, Some(1))
        .hasNext
    }
    e.getMessage should be(
      "Could not infer s3://rawlabs-public-test-data/winter_olympics.csv, please verify that the path is a valid json file")
  }

}
