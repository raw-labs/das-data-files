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

import java.io.File
import java.net.URI

import org.apache.commons.io.FileUtils
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import com.rawlabs.das.datafiles.SparkTestContext
import com.rawlabs.das.datafiles.api.DataFilesTableConfig
import com.rawlabs.das.datafiles.filesystem.{FileCacheManager, FileSystemError}
import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.protocol.das.v1.query.{Operator, Qual, SimpleQual, SortKey}
import com.rawlabs.protocol.das.v1.types.{Value, ValueInt, ValueNull, ValueString}

class CsvTableTest extends AnyFlatSpec with Matchers with SparkTestContext with BeforeAndAfterEach {

  // A small CSV file content for testing
  private val csvContent =
    """id,name
      |1,Alice
      |2,Bob
      |3,
      |4,alice
      |""".stripMargin

  // We'll create a local temp CSV file that "mocked.com/test.csv" will point to
  private val tempCsvFile: File = {
    val f = File.createTempFile("testData-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, csvContent, "UTF-8")
    f
  }

  // A mock FileCacheManager
  private val mockCacheManager = mock(classOf[FileCacheManager])

  private val baseConfig = DataFilesTableConfig(
    name = "testCsv",
    uri = new URI("file://mocked.com/test.csv"),
    format = Some("csv"),
    options = Map("header" -> "true"),
    fileCacheManager = mockCacheManager)

  // 2) Instantiate the CSV table
  private val table = new CsvTable(baseConfig, spark)

  override def beforeEach(): Unit = {
    super.beforeEach()
    reset(mockCacheManager)
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/test.csv"))
      .thenReturn(Right(tempCsvFile.getAbsolutePath))
  }

  behavior of "CsvTable.execute()"

  it should "build a proper tableDefinition with inferred schema" in {
    val defn = table.tableDefinition
    defn.getColumnsCount shouldBe 2 // "id" and "name"
    val col0 = defn.getColumns(0)
    col0.getName shouldBe "id"
    col0.getType.hasInt shouldBe true // or hasLong, depending on inference
    val col1 = defn.getColumns(1)
    col1.getName shouldBe "name"
    col1.getType.hasString shouldBe true
  }

  it should "return rows from CSV when the URL is HTTP" in {
    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)

    val expected = Seq((1, "Alice"), (2, "Bob"), (3, ""), (4, "alice"))
    expected.foreach { case (id, name) =>
      result.hasNext shouldBe true
      val row = result.next()
      row.getColumnsCount shouldBe 2
      row.getColumns(0).getData.getInt.getV shouldBe id
      row.getColumns(1).getData.getString.getV shouldBe name
    }
    result.hasNext shouldBe false
  }

  it should "allow a limit param" in {
    val config = baseConfig.copy(name = "testCsvLimit")
    val table = new CsvTable(config, spark)

    // ask for a limit=2
    val result = table.execute(Nil, Nil, Nil, Some(2L))
    val expected = Seq((1, "Alice"), (2, "Bob"))

    expected.foreach { case (id, name) =>
      result.hasNext shouldBe true
      val row = result.next()
      row.getColumnsCount shouldBe 2
      row.getColumns(0).getData.getInt.getV shouldBe id
      row.getColumns(1).getData.getString.getV shouldBe name
    }
  }

  it should "throw on insert/update/delete" in {
    an[com.rawlabs.das.sdk.DASSdkInvalidArgumentException] should be thrownBy {
      table.insert(com.rawlabs.protocol.das.v1.tables.Row.getDefaultInstance)
    }
    an[com.rawlabs.das.sdk.DASSdkInvalidArgumentException] should be thrownBy {
      table.update(
        com.rawlabs.protocol.das.v1.types.Value.newBuilder().setInt(ValueInt.newBuilder().setV(1)).build(),
        com.rawlabs.protocol.das.v1.tables.Row.getDefaultInstance)
    }
    an[com.rawlabs.das.sdk.DASSdkInvalidArgumentException] should be thrownBy {
      table.delete(com.rawlabs.protocol.das.v1.types.Value.newBuilder().setInt(ValueInt.newBuilder().setV(1)).build())
    }
  }

  it should "filter rows with EQUALS operator" in {
    // Qual => "id = 2"
    val qual = Qual
      .newBuilder()
      .setName("id")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(2))))
      .build()

    val result = table.execute(Seq(qual), Seq.empty, Seq.empty, None)
    result.hasNext shouldBe true
    val row = result.next()
    row.getColumns(0).getData.getInt.getV shouldBe 2
    row.getColumns(1).getData.getString.getV shouldBe "Bob"
    result.hasNext shouldBe false
  }

  it should "filter rows with ILIKE operator" in {
    // Qual => "name ILIKE 'alice'"
    // Should match both "Alice" and "alice"
    val qual = Qual
      .newBuilder()
      .setName("name")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.ILIKE)
          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("alice"))))
      .build()

    val result = table.execute(Seq(qual), Seq.empty, Seq.empty, None)
    result.hasNext shouldBe true
    val row1 = result.next()
    row1.getColumns(0).getData.getInt.getV shouldBe 1
    row1.getColumns(1).getData.getString.getV shouldBe "Alice"

    result.hasNext shouldBe true
    val row2 = result.next()
    row2.getColumns(0).getData.getInt.getV shouldBe 4
    row2.getColumns(1).getData.getString.getV shouldBe "alice"

    result.hasNext shouldBe false
  }

  it should "apply sorting with sortKeys" in {
    // Sort by name descending, nulls first
    val sortKey = SortKey
      .newBuilder()
      .setName("name")
      .setIsReversed(true) // DESC
      .setNullsFirst(true)
      .build()

    val result = table.execute(Nil, Nil, Seq(sortKey), None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList

    // We have name values: "Alice", "Bob", "", "alice"
    // Desc nulls first => "" -> "alice" -> "Bob" -> "Alice"
    rows.map(r => r.getColumns(1).getData.getString.getV) shouldBe List("", "alice", "Bob", "Alice")
  }

  it should "throw a DASSdkInvalidArgumentException when file is too large" in {
    when(mockCacheManager.getLocalPathForUrl(anyString()))
      .thenReturn(Left(FileSystemError.FileTooLarge("file://foo.csv", 9999999999L, 100000000L)))

    intercept[DASSdkInvalidArgumentException] {
      table.execute(Seq.empty, Seq.empty, Seq.empty, None)
    }
  }

  it should "filter rows that match IS NULL" in {
    // "name = null" => row with name missing
    val qual = Qual
      .newBuilder()
      .setName("name")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS)
          .setValue(Value.newBuilder().setNull(ValueNull.getDefaultInstance)))
      .build()

    val result = table.execute(Seq(qual), Seq.empty, Seq.empty, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    // We have one row with name=null => ID=3
    rows.map(r => r.getColumns(0).getData.getInt.getV) shouldBe List(3)
  }

  it should "read CSV with a custom delimiter" in {
    // CSV that uses semicolon delimiter
    val customDelimCsv =
      """id;name;score
        |10;X;3.14
        |11;Y;2.72
        |""".stripMargin
    val f = File.createTempFile("testData-delim-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, customDelimCsv, "UTF-8")

    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/test-delim.csv"),
      options = baseConfig.options ++ Map("delimiter" -> ";"))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/test-delim.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 2

    rows.head.getColumns(0).getData.getInt.getV shouldBe 10
    rows.head.getColumns(1).getData.getString.getV shouldBe "X"
    rows.head.getColumns(2).getData.getDouble.getV shouldBe 3.14
  }

  it should "read CSV with a custom quote and escape" in {
    val tripple = "\"\"\""
    val quotedCsv =
      s"""id,name
        |1,"Alice says ""Hello$tripple
        |2,"Bob"
        |""".stripMargin

    val f = File.createTempFile("testData-quote-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, quotedCsv, "UTF-8")

    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/test-quote.csv"),
      options = baseConfig.options ++ Map("quote" -> "\"", "escape" -> "\""))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/test-quote.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 2

    rows.head.getColumns(1).getData.getString.getV shouldBe """Alice says "Hello""""
  }

  it should "read multiline CSV" in {
    val multilineCsv =
      """id,comment
    |1,"Hello
    |World"
    |2,"Short"
    |""".stripMargin

    val f = File.createTempFile("testData-multi-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, multilineCsv, "UTF-8")

    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/test-multi.csv"),
      options = baseConfig.options ++ Map("multiline" -> "true"))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/test-multi.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 2
    rows.head.getColumns(1).getData.getString.getV shouldBe "Hello\nWorld"
  }

  it should "handle corrupt lines with mode=DROP_MALFORMED" in {
    val badCsv =
      """id,name
    |1,Alice
    |2,Bob,Extra
    |3,Carol
    |""".stripMargin
    val f = File.createTempFile("testData-bad-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, badCsv, "UTF-8")

    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/test-bad.csv"),
      options = baseConfig.options ++ Map("mode" -> "DROPMALFORMED"))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/test-bad.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList

    // The line with "2,Bob,Extra" is dropped
    rows.size shouldBe 2
    rows.head.getColumns(0).getData.getInt.getV shouldBe 1
    rows.last.getColumns(0).getData.getInt.getV shouldBe 3
  }

  it should "parse date columns using date_format" in {
    val dateCsv =
      """col_date
      |2025-01-01
    |2025-01-02
    |""".stripMargin
    val f = File.createTempFile("testData-date-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, dateCsv, "UTF-8")

    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/test-date.csv"),
      options = Map("header" -> "true", "inferSchema" -> "true", "date_format" -> "yyyy-MM-dd"))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/test-date.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val defn = customTable.tableDefinition
    defn.getColumnsCount shouldBe 1
    defn.getColumns(0).getName shouldBe "col_date"
    // If Spark recognized date_format => that col is recognized as a date
    defn.getColumns(0).getType.hasDate shouldBe true
  }
}
