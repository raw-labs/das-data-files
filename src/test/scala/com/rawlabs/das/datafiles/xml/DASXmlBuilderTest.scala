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

package com.rawlabs.das.datafiles.xml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.sdk.{DASSdkInvalidArgumentException, DASSettings}

class DASXmlBuilderTest extends AnyFlatSpec with Matchers {

  // Provide a minimal implicit DASSettings for the build() calls
  implicit val settings: DASSettings = new DASSettings {}

  behavior of "DASXmlHttpBuilder"

  it should "build a DASXmlHttp with an http/https XML table if url starts with http/https" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "https://host/data.xml", "table0_format" -> "xml")

    val sdk = new DASXmlHttpBuilder().build(options)
    sdk shouldBe a[DASXmlHttp]

    val httpXml = sdk.asInstanceOf[DASXmlHttp]
    httpXml.tables.size shouldBe 1
    val (_, table) = httpXml.tables.head
    table shouldBe a[XmlTable]
  }

  it should "throw an exception if url is not http/https for DASXmlHttp" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://some-bucket/data.xml", "table0_format" -> "xml")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASXmlHttpBuilder().build(options)
    }
  }

  // ------------------------------------------------
  behavior of "DASXmlS3Builder"

  it should "build a DASXmlS3 with an s3 XML table if url starts with s3" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "s3://some-bucket/data.xml", "table0_format" -> "xml")

    val sdk = new DASXmlS3Builder().build(options)
    sdk shouldBe a[DASXml]

    val s3Xml = sdk.asInstanceOf[DASXml]
    s3Xml.tables.size shouldBe 1
    val (_, table) = s3Xml.tables.head
    table shouldBe a[XmlTable]
  }

  it should "throw an exception if url does not start with s3 for XML" in {
    val options = Map("nr_tables" -> "1", "table0_url" -> "http://example.com/data.xml", "table0_format" -> "xml")
    an[DASSdkInvalidArgumentException] should be thrownBy {
      new DASXmlS3Builder().build(options)
    }
  }

}
