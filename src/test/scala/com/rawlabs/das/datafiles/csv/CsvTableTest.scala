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
    an[com.rawlabs.das.sdk.DASSdkUnsupportedException] should be thrownBy {
      table.insert(com.rawlabs.protocol.das.v1.tables.Row.getDefaultInstance)
    }
    an[com.rawlabs.das.sdk.DASSdkUnsupportedException] should be thrownBy {
      table.update(
        com.rawlabs.protocol.das.v1.types.Value.newBuilder().setInt(ValueInt.newBuilder().setV(1)).build(),
        com.rawlabs.protocol.das.v1.tables.Row.getDefaultInstance)
    }
    an[com.rawlabs.das.sdk.DASSdkUnsupportedException] should be thrownBy {
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

  it should "ignore leading whitespaces when ignore_leading_white_space = true" in {
    val content =
      """val1,val2
        |   A,    B
        |   X,    Y
        |""".stripMargin
    val f = File.createTempFile("csv-leading-ws-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, content, "UTF-8")

    // Create a config that includes "ignore_leading_white_space" -> "true"
    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/leading_ws.csv"),
      options = baseConfig.options ++ Map("ignore_leading_white_space" -> "true"))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/leading_ws.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList

    // If leading whitespace is ignored, we should get: val1="A", val2="    B" => Actually the "leading" spaces on val2
    // But for demonstration, let's confirm we *don't* see extra spaces on val1
    // If you also set "ignore_trailing_whiteSpace", the trailing spaces might vanish from val2 as well.
    rows.size shouldBe 2
    rows.head.getColumns(0).getData.getString.getV shouldBe "A"
    // We didn't specify ignore_trailing_whiteSpace, so val2 might still contain trailing spaces
  }

  it should "ignore trailing whitespaces when ignore_trailing_whiteSpace = true" in {
    val content =
      """val1,val2
        |Alice    ,Bob
        |Carol   ,Dave
        |""".stripMargin
    val f = File.createTempFile("csv-trailing-ws-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, content, "UTF-8")

    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/trailing_ws.csv"),
      options = baseConfig.options ++ Map("ignore_trailing_whiteSpace" -> "true"))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/trailing_ws.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 2

    // If trailing whitespace is ignored, "Alice    " => "Alice", "Bob    " => "Bob"
    rows.head.getColumns(0).getData.getString.getV shouldBe "Alice"
    rows.head.getColumns(1).getData.getString.getV shouldBe "Bob"
    rows.last.getColumns(0).getData.getString.getV shouldBe "Carol"
    rows.last.getColumns(1).getData.getString.getV shouldBe "Dave"
  }

  it should "treat the configured null_value string as null" in {
    val content =
      """col1,col2
        |1,NULL
        |2,stuff
        |""".stripMargin

    val f = File.createTempFile("csv-nullval-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, content, "UTF-8")

    // Suppose "NULL" text in CSV is actually null
    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/null_val.csv"),
      options = baseConfig.options ++ Map("null_value" -> "NULL"))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/null_val.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 2

    // First row => col2 is null (not the string "NULL")
    rows.head.getColumns(1).getData.hasNull shouldBe true
    // Second row => col2 is "stuff"
    rows.last.getColumns(1).getData.getString.getV shouldBe "stuff"
  }

  it should "treat the configured nan_value string as NaN" in {
    val content =
      """col1
        |NaNSTR
        |1.23
        |""".stripMargin

    val f = File.createTempFile("csv-nanval-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, content, "UTF-8")

    // Suppose "NaNSTR" is recognized as NaN
    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/nan_val.csv"),
      options = baseConfig.options ++ Map("nan_value" -> "NaNSTR", "inferSchema" -> "true"))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/nan_val.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList

    // The first row => col1 => Double.NaN
    val row1Val = rows.head.getColumns(0).getData.getDouble.getV
    row1Val.isNaN shouldBe true

    // The second row => col1 => 1.23
    rows.last.getColumns(0).getData.getDouble.getV shouldBe 1.23 +- 1e-6
  }

  it should "treat the configured positive_inf string as +Infinity" in {
    val content =
      """val
        |POS_INF
        |3.14
        |""".stripMargin

    val f = File.createTempFile("csv-posinf-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, content, "UTF-8")

    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/pos_inf.csv"),
      options = baseConfig.options ++ Map("positive_inf" -> "POS_INF", "inferSchema" -> "true"))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/pos_inf.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 2

    // row0 => Infinity
    val row0Val = rows.head.getColumns(0).getData.getDouble.getV
    row0Val.isPosInfinity shouldBe true

    // row1 => 3.14
    val row1Val = rows.last.getColumns(0).getData.getDouble.getV
    row1Val shouldBe 3.14 +- 1e-5
  }

  it should "treat the configured negative_inf string as -Infinity" in {
    val content =
      """val
        |-INFSTR
        |4.56
        |""".stripMargin

    val f = File.createTempFile("csv-neginf-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, content, "UTF-8")

    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/neg_inf.csv"),
      options = baseConfig.options ++ Map("negative_inf" -> "-INFSTR", "inferSchema" -> "true"))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/neg_inf.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 2

    val row0Val = rows.head.getColumns(0).getData.getDouble.getV
    row0Val.isNegInfinity shouldBe true

    val row1Val = rows.last.getColumns(0).getData.getDouble.getV
    row1Val shouldBe 4.56 +- 1e-6
  }

  it should "respect sampling_ratio for schema inference" in {
    // If sampling_ratio is small, Spark might incorrectly infer the schema if the
    // sample doesn't see certain columns. For demonstration, let's create a CSV
    // with some rows missing columns, etc.
    val content =
      """colA,colB
      |10,
      |20,hello
      |30,
      |40,world
      |""".stripMargin

    val f = File.createTempFile("csv-sampling-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, content, "UTF-8")

    val config = baseConfig.copy(
      uri = new URI("file://mocked.com/sampling_ratio.csv"),
      options = baseConfig.options ++ Map(
        "inferSchema" -> "true",
        "sampling_ratio" -> "0.3" // Only 30% of rows => might see "colB" sometimes
      ))
    when(mockCacheManager.getLocalPathForUrl("file://mocked.com/sampling_ratio.csv"))
      .thenReturn(Right(f.getAbsolutePath))

    val customTable = new CsvTable(config, spark)
    val result = customTable.execute(Nil, Nil, Nil, None)

    // Because sampling is nondeterministic, in practice Spark might or might not
    // see "hello"/"world" as strings in colB. So verifying the final schema is tricky.
    // For demonstration, you can at least confirm it doesn't crash. In a real test,
    // you might do multiple runs or set a known random seed for Spark.
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    rows.size shouldBe 4
  }

}
