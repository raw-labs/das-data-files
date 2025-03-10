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

package com.rawlabs.das.datafiles.json

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

class DasJsonBuilderTest extends AnyFlatSpec with Matchers {

  // Provide a minimal implicit DASSettings for the build() calls
  implicit val settings: DASSettings = new DASSettings {}

  // ------------------------------------------------
  behavior of "DASHttpJsonBuilder"

  it should "build a DASJsonHttp with an http JSON table if url starts with http" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "https://example.com/data.json", "table0_format" -> "json")

    val sdk = new DASJsonHttpBuilder().build(options)
    sdk shouldBe a[DASJsonHttp]

    val httpJson = sdk.asInstanceOf[DASJsonHttp]
    httpJson.tables.size shouldBe 1
    val (_, table) = httpJson.tables.head
    table shouldBe a[JsonTable]
  }

  it should "throw an exception if url does not start with http or https for JSON" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "file:///local/path.json", "table0_format" -> "json")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASJsonHttpBuilder().build(options)
    }
  }

  behavior of "DASJsonS3Builder"

  it should "build a DASJsonS3 with an s3 JSON table if url starts with s3" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://some-bucket/data.json", "table0_format" -> "json")

    val sdk = new DASJsonS3Builder().build(options)
    sdk shouldBe a[DASJsonS3]

    val s3Json = sdk.asInstanceOf[DASJsonS3]
    s3Json.tables.size shouldBe 1
    val (_, table) = s3Json.tables.head
    table shouldBe a[JsonTable]
  }

  it should "throw an exception if url does not start with s3 for JSON" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "http://example.com/data.json", "table0_format" -> "json")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASJsonS3Builder().build(options)
    }
  }

}
