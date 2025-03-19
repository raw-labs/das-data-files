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

import com.rawlabs.das.datafiles.SparkTestContext
import com.rawlabs.das.datafiles.api.DataFilesTableConfig
import com.rawlabs.das.datafiles.filesystem.{BaseFileSystem, FileSystemError}
import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.protocol.das.v1.query.{Operator, Qual, SimpleQual, SortKey}
import com.rawlabs.protocol.das.v1.types.{Value, ValueInt, ValueNull, ValueString}
import org.apache.commons.io.FileUtils
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.net.URI

class CsvTableTest extends AnyFlatSpec with Matchers with SparkTestContext with BeforeAndAfterEach {

  // A small CSV file content for testing
  private val csvContent =
    """id,name
      |1,Alice
      |2,Bob
      |3,
      |4,alice
      |""".stripMargin

  // We'll create a local temp CSV file that the "HTTP" mock will return
  private val tempCsvFile: File = {
    val f = File.createTempFile("testData-", ".csv")
    f.deleteOnExit()
    FileUtils.writeStringToFile(f, csvContent, "UTF-8")
    f
  }

  // A mock HttpFileCache
  private val mockFilesystem = mock(classOf[BaseFileSystem])

  private val config = DataFilesTableConfig(
    name = "testCsv",
    uri = new URI("file://mocked.com/test.csv"),
    format = Some("csv"),
    options = Map("header" -> "true"),
    filesystem = mockFilesystem)

  // 2) Instantiate the CSV table
  private val table = new CsvTable(config, spark)

  override def beforeEach(): Unit = {
    super.beforeEach()
    reset(mockFilesystem)
    // For any call to getLocalFileFor with the given URL, return our local CSV
    when(mockFilesystem.getLocalUrl(ArgumentMatchers.eq("file://mocked.com/test.csv")))
      .thenReturn(Right(tempCsvFile.getAbsolutePath))
  }

  behavior of "CsvTable.execute()"

  it should "build a proper tableDefinition with inferred schema" in {
    val defn = table.tableDefinition
    defn.getColumnsCount shouldBe 2 // "id" and "name"
    val col0 = defn.getColumns(0)
    col0.getName shouldBe "id"
    col0.getType.hasInt shouldBe true  // or hasLong, depending on inference
    val col1 = defn.getColumns(1)
    col1.getName shouldBe "name"
    col1.getType.hasString shouldBe true
  }

  it should "return rows from CSV when the URL is HTTP" in {
    // 1) Build a DataFileConfig with a URL that looks HTTP

    // 3) We call 'execute' with no quals, no columns => return all columns
    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)
    // 4) We'll read rows, line 3 in a null value
    val expected =
      Seq((1, "Alice"), (2, "Bob"), (3, ""), (4, "alice"))

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
    val config = DataFilesTableConfig(
      name = "testCsvLimit",
      uri = new URI("file://mocked.com/test.csv"),
      format = Some("csv"),
      options = Map("header" -> "true"),
      filesystem = mockFilesystem)

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
        com.rawlabs.protocol.das.v1.types.Value
          .newBuilder()
          .setInt(com.rawlabs.protocol.das.v1.types.ValueInt.newBuilder().setV(1))
          .build(),
        com.rawlabs.protocol.das.v1.tables.Row.getDefaultInstance)
    }
    an[com.rawlabs.das.sdk.DASSdkInvalidArgumentException] should be thrownBy {
      table.delete(
        com.rawlabs.protocol.das.v1.types.Value
          .newBuilder()
          .setInt(com.rawlabs.protocol.das.v1.types.ValueInt.newBuilder().setV(1))
          .build())
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
    val idVal = row.getColumns(0).getData.getInt.getV
    val nameVal = row.getColumns(1).getData.getString.getV

    idVal shouldBe 2
    nameVal shouldBe "Bob"
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

    // Expect two matches: "Alice" and "alice"
    result.hasNext shouldBe true
    val row1 = result.next()
    row1.getColumns(0).getData.getInt.getV shouldBe 1 // ID=1
    row1.getColumns(1).getData.getString.getV shouldBe "Alice"

    result.hasNext shouldBe true
    val row2 = result.next()
    row2.getColumns(0).getData.getInt.getV shouldBe 4 // ID=4
    row2.getColumns(1).getData.getString.getV shouldBe "alice"

    result.hasNext shouldBe false
  }

  it should "apply sorting with sortKeys" in {

    // Sort by name descending
    val sortKey = SortKey
      .newBuilder()
      .setName("name")
      .setIsReversed(true) // DESC
      .setNullsFirst(true)
      .build()

    val result = table.execute(Nil, Nil, Seq(sortKey), None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList

    // We have name values:"Alice", "Bob", null,  "alice"
    // Desc nulls first=> null -> "alice" -> "Bob" -> "Alice"

    rows.map(r => r.getColumns(1).getData.getString.getV) shouldBe List("", "alice", "Bob", "Alice")
  }

  it should "throw a DASSdkInvalidArgumentException when file is too large" in {
    when(mockFilesystem.getLocalUrl(anyString()))
      .thenReturn(Left(FileSystemError.FileTooLarge("file://foo.csv", 9999999999L, 100000000L)))

    intercept[DASSdkInvalidArgumentException] {
      table.execute(Seq.empty, Seq.empty, Seq.empty, None)
    }
  }

  it should "filter rows that match IS NULL" in {
    // Suppose your CSV/JSON includes some row with null in the 'name' column.
    // Then we do "WHERE name = null"
    val qual = Qual
      .newBuilder()
      .setName("name")
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(Operator.EQUALS) // or NOT_EQUALS
          .setValue(Value.newBuilder().setNull(ValueNull.getDefaultInstance)))
      .build()

    val result = table.execute(Seq(qual), Seq.empty, Seq.empty, None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList
    // We have one row with name=null, row 3
    rows.map(r => r.getColumns(0).getData.getInt.getV) shouldBe List(3)
  }

}
