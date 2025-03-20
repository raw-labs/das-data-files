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

package com.rawlabs.das.datafiles.json

import com.rawlabs.das.datafiles.SparkTestContext
import com.rawlabs.das.datafiles.api.DataFilesTableConfig
import com.rawlabs.das.datafiles.filesystem.FileCacheManager
import com.rawlabs.protocol.das.v1.query.Qual
import org.apache.commons.io.FileUtils
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.net.URI

class JsonTableTest extends AnyFlatSpec with Matchers with SparkTestContext with BeforeAndAfterAll {

  private val jsonContent =
    """[
      |  {"id":1, "name":"Alice"},
      |  {"id":2, "name":"Bob"},
      |  {"id":3, "name":"Carol"}
      |]
      |""".stripMargin

  private val jsonLinesContent =
    """{"id":1, "name":"Alice"}
      |{"id":2, "name":"Bob"}
      |{"id":3, "name":"Carol"}""".stripMargin

  private val tempJsonFile: File = {
    val f = File.createTempFile("testData-", ".json")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, jsonContent, "UTF-8")
    f
  }

  private val tempJsonLinesFile: File = {
    val f = File.createTempFile("testLinesData-", ".json")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, jsonLinesContent, "UTF-8")
    f
  }

  // Mock HttpFileCache
  private val mockCacheManager = mock(classOf[FileCacheManager])

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(mockCacheManager.getLocalPathForUrl(ArgumentMatchers.eq("file://mocked.com/test.json")))
      .thenReturn(Right(tempJsonFile.getAbsolutePath))

    when(mockCacheManager.getLocalPathForUrl(ArgumentMatchers.eq("file://mocked.com/test-lines.json")))
      .thenReturn(Right(tempJsonLinesFile.getAbsolutePath))

  }

  behavior of "JsonTable"

  it should "load rows from a JSON file" in {
    val config = DataFilesTableConfig(
      name = "testJson",
      uri = new URI("file://mocked.com/test.json"),
      format = Some("json"),
      options = Map.empty, // default to read json is multiline=true (normal json and not json lines)
      fileCacheManager = mockCacheManager)

    val table = new JsonTable(config, spark)

    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)

    result.hasNext shouldBe true
    val row1 = result.next()
    row1.getColumnsCount shouldBe 2
    row1.getColumns(0).getName shouldBe "id"
    row1.getColumns(0).getData.getLong.getV shouldBe 1
    row1.getColumns(1).getName shouldBe "name"
    row1.getColumns(1).getData.getString.getV shouldBe "Alice"

    result.hasNext shouldBe true
    val row2 = result.next()
    row2.getColumns(0).getData.getLong.getV shouldBe 2
    row2.getColumns(1).getData.getString.getV shouldBe "Bob"

    result.hasNext shouldBe true
    val row3 = result.next()
    row3.getColumns(0).getData.getLong.getV shouldBe 3
    row3.getColumns(1).getData.getString.getV shouldBe "Carol"

    result.hasNext shouldBe false
  }

  it should "load rows from a JSON Lines file" in {
    val config = DataFilesTableConfig(
      name = "testJson",
      uri = new URI("file://mocked.com/test-lines.json"),
      format = Some("json"),
      options = Map("multiLine" -> "false"), // for json lines file this has to be false
      fileCacheManager = mockCacheManager)

    val table = new JsonTable(config, spark)

    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)

    result.hasNext shouldBe true
    val row1 = result.next()
    row1.getColumnsCount shouldBe 2
    row1.getColumns(0).getName shouldBe "id"
    row1.getColumns(0).getData.getLong.getV shouldBe 1
    row1.getColumns(1).getName shouldBe "name"
    row1.getColumns(1).getData.getString.getV shouldBe "Alice"

    result.hasNext shouldBe true
    val row2 = result.next()
    row2.getColumns(0).getData.getLong.getV shouldBe 2
    row2.getColumns(1).getData.getString.getV shouldBe "Bob"

    result.hasNext shouldBe true
    val row3 = result.next()
    row3.getColumns(0).getData.getLong.getV shouldBe 3
    row3.getColumns(1).getData.getString.getV shouldBe "Carol"

    result.hasNext shouldBe false
  }
}
