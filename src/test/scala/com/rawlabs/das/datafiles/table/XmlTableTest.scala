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

package com.rawlabs.das.datafiles.table

import java.io.File

import org.apache.commons.io.FileUtils
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.datafiles.{DataFileConfig, HttpFileCache, SparkTestContext}
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

  private val mockCache = mock(classOf[HttpFileCache])

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(
      mockCache.acquireFor(
        anyString(),
        ArgumentMatchers.eq("http://mocked.com/test.xml"),
        any[Option[String]](),
        any[Map[String, String]](),
        anyInt())).thenReturn(tempXmlFile.getAbsolutePath)
  }

  "XmlTable" should "load rows from an XML file" in {
    val config = DataFileConfig(
      name = "testXml",
      url = "http://mocked.com/test.xml",
      format = Some("xml"),
      options = Map("root_tag" -> "rows", "row_tag" -> "row"))

    val table = new XmlTable(config, spark, mockCache)
    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)

    val rows = Iterator
      .continually(result)
      .takeWhile(_.hasNext)
      .map { x =>
        val row = x.next()
        row.getColumns(0).getName shouldBe "id"
        row.getColumns(1).getName shouldBe "name"
        (row.getColumns(0).getData.getLong.getV, row.getColumns(1).getData.getString.getV)
      }
      .toList

    assert(rows == List((1, "Alice"), (2, "Bob")))

  }

}
