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

package com.rawlabs.das.datafiles

import java.io.File

import org.apache.commons.io.FileUtils
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.protocol.das.v1.query.{Operator, Qual, SimpleQual, SortKey}
import com.rawlabs.protocol.das.v1.types.{Value, ValueInt, ValueString}

class CsvTableTest extends AnyFlatSpec with Matchers with SparkTestContext with BeforeAndAfterAll {

  // A small CSV file content for testing
  private val csvContent =
    """id,name
      |1,Alice
      |2,Bob
      |3,Carol
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
  private val mockCache = mock(classOf[HttpFileCache])

  override def beforeAll(): Unit = {
    super.beforeAll()

    // For any call to getLocalFileFor with the given URL, return our local CSV
    when(
      mockCache.acquireFor(
        anyString(), // method
        ArgumentMatchers.eq("http://mocked.com/test.csv"), // remoteUrl
        any[Option[String]](),
        any[Map[String, String]](),
        any[Int]())).thenReturn(tempCsvFile.getAbsolutePath)
  }

  behavior of "CsvTable.execute()"

  it should "return rows from CSV when the URL is HTTP" in {
    // 1) Build a DataFileConfig with a URL that looks HTTP
    val config = DataFileConfig(
      name = "testCsv",
      url = "http://mocked.com/test.csv",
      format = Some("csv"),
      options = Map("header" -> "true"))

    // 2) Instantiate the CSV table
    val table = new CsvTable(config, spark, mockCache)

    // 3) We call 'execute' with no quals, no columns => return all columns
    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)
    // 4) We'll read rows
    val expected =
      Seq((1, "Alice"), (2, "Bob"), (3, "Carol"), (4, "alice"))

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
    val config = DataFileConfig(
      name = "testCsvLimit",
      url = "http://mocked.com/test.csv",
      format = Some("csv"),
      options = Map("header" -> "true"))

    val table = new CsvTable(config, spark, mockCache)

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
    val config = DataFileConfig(
      name = "readOnlyTest",
      url = "http://mocked.com/test.csv",
      format = Some("csv"),
      options = Map("header" -> "true"))
    val table = new CsvTable(config, spark, mockCache)

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
    val config = DataFileConfig(
      name = "testPushdownCsv",
      url = "http://mocked.com/test.csv",
      format = Some("csv"),
      options = Map("header" -> "true"))

    val table = new CsvTable(config, spark, mockCache)

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
    val config = DataFileConfig(
      name = "testILikeCsv",
      url = "http://mocked.com/test.csv",
      format = Some("csv"),
      options = Map("header" -> "true"))

    val table = new CsvTable(config, spark, mockCache)

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
    val config = DataFileConfig(
      name = "testSortCsv",
      url = "http://mocked.com/test.csv",
      format = Some("csv"),
      options = Map("header" -> "true"))

    val table = new CsvTable(config, spark, mockCache)

    // Sort by name descending
    val sortKey = SortKey
      .newBuilder()
      .setName("name")
      .setIsReversed(true) // DESC
      .build()

    val result = table.execute(Nil, Nil, Seq(sortKey), None)
    val rows = Iterator.continually(result).takeWhile(_.hasNext).map(_.next()).toList

    // We have name values: "alice", "Alice", "Bob", "Carol"
    // Desc sort by name => "alice" > "Carol" > "Bob" > "Alice" in pure string ordering?
    // Actually, in ASCII sorting, uppercase < lowercase. So let's see the order:
    //   "alice" (ASCII 97)
    //   "Carol" (ASCII 67 => 'C' is after 'a'? Actually 'C' is 67)
    //   "Bob"   (ASCII 66 => 'B' is 66)
    //   "Alice" (ASCII 65 => 'A' is 65)
    //
    // Desc => "alice" -> "Carol" -> "Bob" -> "Alice"

    rows.map(r => r.getColumns(1).getData.getString.getV) shouldBe List("alice", "Carol", "Bob", "Alice")
  }

}
