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

package com.rawlabs.das.datafiles.parquet

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

class DasParquetBuilderTest extends AnyFlatSpec with Matchers {

  // Provide a minimal implicit DASSettings for the build() calls
  implicit val settings: DASSettings = new DASSettings {}

  // ------------------------------------------------
  behavior of "DASParquetHttpBuilder"

  it should "build a DASParquetHttp with an http Parquet table if url starts with http" in {
    val options =
      Map("nr_tables" -> "1", "table0_url" -> "http://example.com/data.parquet", "table0_format" -> "parquet")

    val sdk = new DASParquetHttpBuilder().build(options)
    sdk shouldBe a[DASParquetHttp]

    val httpParquet = sdk.asInstanceOf[DASParquetHttp]
    httpParquet.tables.size shouldBe 1
    val (_, table) = httpParquet.tables.head
    table shouldBe a[ParquetTable]
  }

  it should "throw an exception if url does not start with http or https for Parquet" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://bucket/file.parquet", "table0_format" -> "parquet")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASParquetHttpBuilder().build(options)
    }
  }

  behavior of "DASParquetS3Builder"

  it should "build a DASParquetS3 with an s3 Parquet table if url starts with s3" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://some-bucket/data.parquet", "table0_format" -> "parquet")

    val sdk = new DASParquetS3Builder().build(options)
    sdk shouldBe a[DASParquetS3]

    val s3Parquet = sdk.asInstanceOf[DASParquetS3]
    s3Parquet.tables.size shouldBe 1
    val (_, table) = s3Parquet.tables.head
    table shouldBe a[ParquetTable]
  }

  it should "throw an exception if url does not start with s3 for Parquet" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "https://host/data.parquet", "table0_format" -> "parquet")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASParquetS3Builder().build(options)
    }
  }
}
