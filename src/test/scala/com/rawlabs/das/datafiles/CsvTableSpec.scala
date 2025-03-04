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

package com.rawlabs.das.datafiles

import com.rawlabs.protocol.das.v1.query.Qual
import org.apache.commons.io.FileUtils
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class CsvTableSpec
  extends AnyFlatSpec
    with Matchers
    with SparkTestContext
    with BeforeAndAfterAll {

  // A small CSV file content for testing
  private val csvContent =
    """id,name
      |1,Alice
      |2,Bob
      |3,Carol
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
      mockCache.getLocalFileFor(
        anyString(),             // method
        ArgumentMatchers.eq("http://mocked.com/test.csv"),  // remoteUrl
        any[Option[String]](),
        any[Map[String, String]](),
        any[HttpConnectionOptions]()
      )
    ).thenReturn(tempCsvFile)
  }

  behavior of "CsvTable.execute()"

  it should "return rows from CSV when the URL is HTTP" in {
    // 1) Build a DataFileConfig with a URL that looks HTTP
    val config = DataFileConfig(
      name    = "testCsv",
      url     = "http://mocked.com/test.csv",
      format  = Some("csv"),
      options = Map("header" -> "true"),
      httpOptions = HttpConnectionOptions(
        followRedirects = true,
        connectTimeout = 10000,
        readTimeout    = 10000,
        sslTRustAll    = false
      )
    )

    // 2) Instantiate the CSV table
    val table = new CsvTable(config, spark, mockCache)

    // 3) We call 'execute' with no quals, no columns => return all columns
    val result = table.execute(Seq.empty[Qual], Seq.empty[String], Seq.empty, None)

    // 4) We'll read rows
    result.hasNext shouldBe true
    val row1 = result.next() // Row => we can check the first row's columns

    row1.getColumnsCount shouldBe 2  // "id", "name"

    // for clarity, let's parse them
    val colId_1   = row1.getColumns(0).getData.getInt.getV
    val colName_1 = row1.getColumns(1).getData.getString.getV
    colId_1 shouldBe 1
    colName_1 shouldBe "Alice"

    result.hasNext shouldBe true
    val row2 = result.next()
    val colId_2   = row2.getColumns(0).getData.getInt.getV
    val colName_2 = row2.getColumns(1).getData.getString.getV
    colId_2 shouldBe 2
    colName_2 shouldBe "Bob"

    result.hasNext shouldBe true
    val row3 = result.next()
    val colId_3   = row3.getColumns(0).getData.getInt.getV
    val colName_3 = row3.getColumns(1).getData.getString.getV
    colId_3 shouldBe 3
    colName_3 shouldBe "Carol"

    result.hasNext shouldBe false
  }

  it should "allow a limit param" in {
    val config = DataFileConfig(
      name    = "testCsvLimit",
      url     = "http://mocked.com/test.csv",
      format  = Some("csv"),
      options = Map("header" -> "true"),
      httpOptions = HttpConnectionOptions(true,10000,10000,false)
    )

    val table = new CsvTable(config, spark, mockCache)

    // ask for a limit=2
    val result = table.execute(Nil, Nil, Nil, Some(2L))

    result.hasNext shouldBe true
    val row1 = result.next()
    println(s"row1: $row1")
    result.hasNext shouldBe true
    val row2 = result.next()
    println(s"row2: $row2")
    result.hasNext shouldBe false
  }
}
