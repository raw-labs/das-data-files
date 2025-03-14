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

package com.rawlabs.das.datafiles.csv

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

class DasCsvBuilderTest extends AnyFlatSpec with Matchers {

  // Provide a minimal implicit DASSettings for the build() calls
  implicit val settings: DASSettings = new DASSettings {}

  behavior of "DasCsvHttpBuilder"

  it should "build a DasCsvHttp with an http CSV table if url starts with http" in {
    val options = Map(
      "nr_tables" -> "1",
      "table0_url" -> "http://example.com/data.csv",
      "table0_format" -> "csv"
      // plus any other required config keys...
    )

    val sdk = new DASCsvHttpBuilder().build(options)
    sdk shouldBe a[DASCsvHttp]

    val httpCsv = sdk.asInstanceOf[DASCsvHttp]
    httpCsv.tables.size shouldBe 1
    val (_, table) = httpCsv.tables.head
    table shouldBe a[CsvTable]
  }

  it should "throw an exception if url does not start with http or https" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://mybucket/data.csv", "table0_format" -> "csv")

    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASCsvHttpBuilder().build(options)
    }
  }

  behavior of "DasCsvS3Builder"

  it should "build a DASCsvS3 with an s3 CSV table if url starts with s3" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://mybucket/data.csv", "table0_format" -> "csv")

    val sdk = new DASCsvS3Builder().build(options)
    sdk shouldBe a[DASCsv]

    val s3Csv = sdk.asInstanceOf[DASCsv]
    s3Csv.tables.size shouldBe 1
    val (_, table) = s3Csv.tables.head
    table shouldBe a[CsvTable]
  }

  it should "throw an exception if url does not start with s3 for CSV" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "https://example.com/data.csv", "table0_format" -> "csv")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASCsvS3Builder().build(options)
    }
  }
}
