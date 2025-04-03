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

package com.rawlabs.das.datafiles.xml

import java.io.File
import java.net.URI

import org.apache.commons.io.FileUtils
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.rawlabs.das.datafiles.api.DataFilesTableConfig
import com.rawlabs.das.datafiles.filesystem.FileCacheManager
import com.rawlabs.protocol.das.v1.query.Qual

class XmlTableTest extends AnyFlatSpec with Matchers with SparkTestContext with BeforeAndAfterAll {

  private val xmlContent =
    """<rows>
      |  <row><id>1</id><name>Alice</name></row>
      |  <row><id>2</id><name>Bob</name></row>
      |</rows>
      |""".stripMargin

  private val tempXmlFile: File = {
    val f = File.createTempFile("testData-", ".xml")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, xmlContent, "UTF-8")
    f
  }

  private val mockCacheManager = mock(classOf[FileCacheManager])
  private val url = "file://mocked/test.xml"

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(mockCacheManager.getLocalPathForUrl(ArgumentMatchers.eq(url)))
      .thenReturn(Right(tempXmlFile.getAbsolutePath))
  }

  "XmlTable" should "load rows from an XML file" in {
    val config = DataFilesTableConfig(
      uri = new URI(url),
      name = "testXml",
      format = Some("xml"),
      options = Map("row_tag" -> "row"),
      fileCacheManager = mockCacheManager)

    val table = new XmlTable(config, spark)
    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)

    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows should have size 2

    // First row: id=1, name=Alice
    rows.head.getColumns(0).getName shouldBe "id"
    rows.head.getColumns(0).getData.getLong.getV shouldBe 1
    rows.head.getColumns(1).getName shouldBe "name"
    rows.head.getColumns(1).getData.getString.getV shouldBe "Alice"

    // Second row: id=2, name=Bob
    rows(1).getColumns(0).getData.getLong.getV shouldBe 2
    rows(1).getColumns(1).getData.getString.getV shouldBe "Bob"
  }

  it should "use attribute_prefix to capture XML attributes" in {
    val withAttributes =
      """<people>
        |  <person id="101" type="student">
        |    <name>Alice</name>
        |  </person>
        |  <person id="202" type="teacher">
        |    <name>Bob</name>
        |  </person>
        |</people>
        |""".stripMargin

    val f = File.createTempFile("testData-attrs-", ".xml")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, withAttributes, "UTF-8")

    when(mockCacheManager.getLocalPathForUrl("file://mocked/attrs.xml"))
      .thenReturn(Right(f.getAbsolutePath))

    val config = DataFilesTableConfig(
      uri = new URI("file://mocked/attrs.xml"),
      name = "testXmlAttrs",
      format = Some("xml"),
      options = Map("row_tag" -> "person", "attribute_prefix" -> "@", "root_tag" -> "people"),
      fileCacheManager = mockCacheManager)

    val table = new XmlTable(config, spark)
    val result = table.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 2

    // We expect columns: @id, @type, name
    rows.head.getColumns(0).getName shouldBe "@id"
    rows.head.getColumns(0).getData.getLong.getV shouldBe 101L
    rows.head.getColumns(1).getName shouldBe "@type"
    rows.head.getColumns(1).getData.getString.getV shouldBe "student"
    rows.head.getColumns(2).getName shouldBe "name"
    rows.head.getColumns(2).getData.getString.getV shouldBe "Alice"
  }

  it should "treat empty values as nulls if treat_empty_values_as_nulls = true" in {
    val emptyValXml =
      """<root>
        |  <item><title></title></item>
        |</root>
        |""".stripMargin

    val f = File.createTempFile("testData-empty-", ".xml")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, emptyValXml, "UTF-8")

    when(mockCacheManager.getLocalPathForUrl("file://mocked/empty.xml"))
      .thenReturn(Right(f.getAbsolutePath))

    val config = DataFilesTableConfig(
      uri = new URI("file://mocked/empty.xml"),
      name = "testXmlEmptyAsNull",
      format = Some("xml"),
      options = Map("row_tag" -> "item", "treat_empty_values_as_nulls" -> "true"),
      fileCacheManager = mockCacheManager)

    val table = new XmlTable(config, spark)
    val result = table.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 1

    // The column might be "title" with a null value
    rows.head.getColumns(0).getName shouldBe "title"
    rows.head.getColumns(0).getData.hasNull shouldBe true
  }

  it should "ignore surrounding spaces if ignore_surrounding_spaces = true" in {
    val spaceXml =
      """<things>
        |  <thing><greeting>  Hello  </greeting></thing>
        |</things>
        |""".stripMargin

    val f = File.createTempFile("testData-spaces-", ".xml")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, spaceXml, "UTF-8")

    when(mockCacheManager.getLocalPathForUrl("file://mocked/spaces.xml"))
      .thenReturn(Right(f.getAbsolutePath))

    val config = DataFilesTableConfig(
      uri = new URI("file://mocked/spaces.xml"),
      name = "testXmlSpaces",
      format = Some("xml"),
      options = Map("row_tag" -> "thing", "ignore_surrounding_spaces" -> "true"),
      fileCacheManager = mockCacheManager)

    val table = new XmlTable(config, spark)
    val result = table.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 1

    // Typically spark-xml puts textual content in _VALUE if there's no child element
    rows.head.getColumns(0).getName shouldBe "greeting"
    rows.head.getColumns(0).getData.getString.getV shouldBe "Hello"
  }
}
