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

import java.io.File
import java.net.URI

import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.commons.io.FileUtils
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.datafiles.SparkTestContext
import com.rawlabs.das.datafiles.api.DataFilesTableConfig
import com.rawlabs.das.datafiles.filesystem.FileCacheManager
import com.rawlabs.protocol.das.v1.query.Qual

class JsonTableTest extends AnyFlatSpec with Matchers with SparkTestContext with BeforeAndAfterAll {

  private val jsonContent =
    """[
      |  {"id":1, "name":"Alice"},
      |  {"id":2, "name":"Bob"},
      |  {"id":3, "name":"Carol"}
      |]""".stripMargin

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

  private val mockCacheManager = mock(classOf[FileCacheManager])

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/test.json"))
      .thenReturn(Right(tempJsonFile.getAbsolutePath))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/test-lines.json"))
      .thenReturn(Right(tempJsonLinesFile.getAbsolutePath))
  }

  behavior of "JsonTable"

  it should "load rows from a JSON file" in {
    val config = DataFilesTableConfig(
      name = "testJson",
      uri = new URI("file://mocked.com/test.json"),
      format = Some("json"),
      pathOptions = Map.empty, // default multiLine -> "true" in the code
      fileCacheManager = mockCacheManager,
      globalOptions = Map.empty)

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
      pathOptions = Map("multiLine" -> "false"),
      fileCacheManager = mockCacheManager,
      globalOptions = Map.empty)

    val table = new JsonTable(config, spark)
    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)

    result.hasNext shouldBe true
    val row1 = result.next()
    row1.getColumns(0).getData.getLong.getV shouldBe 1
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

  it should "allow comments in JSON if allow_comments=true" in {
    val withComments =
      """[
        |  // This is a comment
        |  {"id":10, "name":"X"},
        |  // Another comment
        |  {"id":20, "name":"Y"}
        |]""".stripMargin

    val f = File.createTempFile("testData-comments-", ".json")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, withComments, "UTF-8")

    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/comments.json"))
      .thenReturn(Right(f.getAbsolutePath))

    val config = DataFilesTableConfig(
      name = "testJsonComments",
      uri = new URI("file://mocked.com/comments.json"),
      format = Some("json"),
      pathOptions = Map("allow_comments" -> "true"),
      fileCacheManager = mockCacheManager,
      globalOptions = Map.empty)

    val table = new JsonTable(config, spark)
    val result = table.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 2
    rows.head.getColumns(0).getData.getLong.getV shouldBe 10
    rows.last.getColumns(0).getData.getLong.getV shouldBe 20
  }

  it should "use drop_field_if_all_null=true to remove columns that are null in all rows" in {
    val allNullJson =
      """[
        |  {"id":1, "maybe":null},
        |  {"id":2, "maybe":null}
        |]""".stripMargin

    val f = File.createTempFile("testData-allnull-", ".json")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, allNullJson, "UTF-8")

    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/allnull.json"))
      .thenReturn(Right(f.getAbsolutePath))

    val config = DataFilesTableConfig(
      name = "testJsonAllNull",
      uri = new URI("file://mocked.com/allnull.json"),
      format = Some("json"),
      pathOptions = Map("multiLine" -> "true", "drop_field_if_all_null" -> "true"),
      fileCacheManager = mockCacheManager,
      globalOptions = Map.empty)

    val table = new JsonTable(config, spark)
    val defn = table.tableDefinition
    // "id" should remain, "maybe" might get dropped if truly null in all rows
    defn.getColumnsCount shouldBe 1
    defn.getColumns(0).getName shouldBe "id"
  }

  it should "put corrupt records in a special column when using column_name_of_corrupt_record" in {
    val badJson =
      """{"id":1,"name":"Alice"}
        |{"id":2,"name" "Bob"} // Missing colon
        |{"id":3,"name":"Carol"}""".stripMargin

    val f = File.createTempFile("testBadJson-", ".json")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, badJson, "UTF-8")

    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/bad.json"))
      .thenReturn(Right(f.getAbsolutePath))

    val config = DataFilesTableConfig(
      name = "testBadJson",
      uri = new URI("file://mocked.com/bad.json"),
      format = Some("json"),
      pathOptions =
        Map("multiLine" -> "false", "mode" -> "PERMISSIVE", "column_name_of_corrupt_record" -> "_corrupt_data"),
      fileCacheManager = mockCacheManager,
      globalOptions = Map.empty)

    val table = new JsonTable(config, spark)
    val result = table.execute(Seq.empty, Seq.empty, Seq.empty, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList

    rows.size shouldBe 3
    // The second line is corrupted, so we'd expect `_corrupt_data` field to show that line
    val row2 = rows(1)
    val fieldNames = row2.getColumnsList
    fieldNames.toString should include("_corrupt_data")
  }

  it should "infer all primitive values as strings if primitives_as_string = true" in {
    // We'll create a JSON lines file with int, float, boolean, etc.
    val multiTypeJson =
      """{"intVal":123, "floatVal":3.14, "boolVal":true}
        |{"intVal":999, "floatVal":2.71828, "boolVal":false}""".stripMargin

    val f = File.createTempFile("testData-primitives-", ".json")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, multiTypeJson, "UTF-8")

    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/primitives.json"))
      .thenReturn(Right(f.getAbsolutePath))

    val config = DataFilesTableConfig(
      name = "testJsonPrimitivesAsString",
      uri = new URI("file://mocked.com/primitives.json"),
      format = Some("json"),
      pathOptions = Map(
        "multiLine" -> "false",
        "primitives_as_string" -> "true" // Our new option
      ),
      fileCacheManager = mockCacheManager,
      globalOptions = Map.empty)

    val table = new JsonTable(config, spark)
    val result = table.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 2

    // If primitives_as_string took effect, Spark sees "intVal", "floatVal", "boolVal" as strings
    // e.g. "123", "3.14", "true"
    val row1 = rows.head
    row1.getColumnsList.asScala.map(x => (x.getName, x.getData.getString.getV)).toMap shouldBe Map(
      "intVal" -> "123",
      "floatVal" -> "3.14",
      "boolVal" -> "true")
  }

  it should "allow unquoted field names if allow_unquoted_field_names = true" in {
    // We'll create JSON lines with unquoted field names: e.g.  { foo:123 }
    val unquotedJson =
      """{ foo: 10, bar:true }
        |{ foo: 20, bar:false }""".stripMargin

    val f = File.createTempFile("testData-unquoted-", ".json")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, unquotedJson, "UTF-8")

    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/unquoted.json"))
      .thenReturn(Right(f.getAbsolutePath))

    val config = DataFilesTableConfig(
      name = "testJsonUnquoted",
      uri = new URI("file://mocked.com/unquoted.json"),
      format = Some("json"),
      pathOptions = Map("multiLine" -> "false", "allow_unquoted_field_names" -> "true"),
      fileCacheManager = mockCacheManager,
      globalOptions = Map.empty)

    val table = new JsonTable(config, spark)
    val result = table.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList

    rows.size shouldBe 2
    // Now Spark should parse them as valid fields "foo", "bar" (both unquoted)
    val row1 = rows.head
    row1.getColumnsList.asScala.map { x =>
      x.getName match {
        case "foo" => x.getName -> x.getData.getLong.getV
        case "bar" => x.getName -> x.getData.getBool.getV
      }
    }.toMap shouldBe Map("foo" -> 10, "bar" -> true)
  }

}
